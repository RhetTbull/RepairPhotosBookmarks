"""Repair 'Missing File' errors in Photos caused by moving referenced files to a different drive"""

import itertools
import os
import pathlib
import plistlib
import sqlite3
import subprocess
import sys
import time
import urllib

import click
import CoreFoundation
import objc
from Foundation import kCFAllocatorDefault
from mac_alias import Bookmark, kBookmarkPath
from photoscript import PhotosLibrary
from photoscript.utils import ditto

_verbose = 0

TEMPLATE_DIRECTORY = "template_libraries"
TEMPLATE_LIBRARY = "osxphotos_temporary_working_library.photoslibrary"
TEMP_LIBRARY_SENTINEL_ALBUM = "ZZZ_OSXPHOTOS_SENTINEL_ZZZ"

# seconds to sleep after quitting/activating Photos
SLEEP_TIME_AFTER_QUIT = 5
SLEEP_TIME_AFTER_ACTIVATE = 10


def get_temp_photos_library_dir():
    # is picture folder always here independent of locale or language?
    picture_folder = pathlib.Path("~/Pictures").expanduser()
    if not picture_folder.is_dir():
        raise FileNotFoundError(f"Invalid picture folder: '{picture_folder}'")

    return picture_folder / TEMPLATE_LIBRARY


def copy_temporary_photos_library():
    """copy the template library and open Photos, returns path to copied library"""
    src = pathlib.Path(TEMPLATE_DIRECTORY) / TEMPLATE_LIBRARY

    dest = get_temp_photos_library_dir()
    ditto(src, dest)
    return str(dest)


def verify_temp_library_signature():
    """Verify that the loaded library is actually the temporary working library"""
    # return PhotosLibrary().album(TEMP_LIBRARY_SENTINEL_ALBUM) is not None
    return PhotosLibrary().album(TEMP_LIBRARY_SENTINEL_ALBUM) is not None


def photos_is_running() -> bool:
    """Returns True if current user is running Photos, otherwise False"""
    # Note: use subprocess because psutil doesn't work on M1 (see #3)
    user_name = subprocess.check_output(["id", "-un"]).decode("utf-8").strip()
    output = (
        subprocess.check_output(["ps", "-ax", "-o", "user", "-o", "command"])
        .decode("utf-8")
        .splitlines()
    )
    return any(
        proc[0] == user_name and "Photos.app" in proc[1]
        for proc in [line.split(" ", 1) for line in output]
    )


def get_volume_uuid(path: str) -> str:
    """Returns the volume UUID for the given path or None if not found"""
    try:
        output = subprocess.check_output(["diskutil", "info", "-plist", path])
        plist = plistlib.loads(output)
        return plist.get("VolumeUUID", None)
    except subprocess.CalledProcessError as e:
        return None


def open_sqlite_db(fname: str):
    """Open sqlite database and return connection to the database"""
    try:
        conn = sqlite3.connect(f"{fname}")
        c = conn.cursor()
    except sqlite3.Error as e:
        raise OSError(f"Error opening {fname}: {e}") from e
    return (conn, c)


def resolve_bookmark_path(bookmark_data: bytes) -> str:
    """Get the path from a CFURL file bookmark
    This works without calling CFURLCreateByResolvingBookmarkData
    which fails if the target file does not exist
    """
    try:
        bookmark = Bookmark.from_bytes(bookmark_data)
    except Exception as e:
        raise ValueError(f"Invalid bookmark: {e}") from e
    path_components = bookmark.get(kBookmarkPath, None)
    if not path_components:
        return None
    return f"/{os.path.join(*path_components)}"


# TODO Remove this in the future as it's slower and not used!
def _resolve_cfdata_bookmark(bookmark: bytes) -> str:
    """Resolve a bookmark stored as a serialized CFData object into a path str"""

    with objc.autorelease_pool():
        # use CFURLCreateByResolvingBookmarkData to de-serialize bookmark data into a CFURLRef
        url = CoreFoundation.CFURLCreateByResolvingBookmarkData(
            kCFAllocatorDefault, bookmark, 0, None, None, None, None
        )

        # the CFURLRef we got is a struct that python treats as an array
        # I'd like to pass this to CFURLGetFileSystemRepresentation to get the path but
        # CFURLGetFileSystemRepresentation barfs when it gets an array from python instead of expected struct
        # first element is the path string in form:
        # file:///Users/username/Pictures/Photos%20Library.photoslibrary/
        urlstr = url[0].absoluteString() if url[0] else None

        # get detailed info about the bookmark for reverse engineering
        # resources = CoreFoundation.CFURLCreateResourcePropertiesForKeysFromBookmarkData(
        #     None,
        #     ["NSURLBookmarkDetailedDescription"],
        #     bookmark,
        # )
        # print(f"{resources['NSURLBookmarkDetailedDescription']}")

        # now coerce the file URI back into an OS path
        # surely there must be a better way
        if not urlstr:
            raise ValueError("Could not resolve bookmark")

        return os.path.normpath(
            urllib.parse.unquote(urllib.parse.urlparse(urlstr).path)
        )


def read_file_locations_from_photos_database(photos_db_path):
    """read locations for referenced files from Photos database, returns dict of file paths by pk"""
    results = _read_zfilesystem_bookmark_from_photos_database(photos_db_path)

    # read all the bookmarks
    # TODO: Do we really need to resolve the bookmarks or can we construct the path from the ZFILESYSTEMBOOKMARK.ZPATHRELATIVETOVOLUME and ZFILESYSTEMVOLUME.ZNAME fields?
    # Note, it's best / simplest to resolve the bookmarks, although the other info could be a backup
    referenced_files = {}
    for pk, pathstr, bookmark_data in results:
        try:
            bookmark_path = resolve_bookmark_path(bookmark_data)
            referenced_files[pk] = bookmark_path
            if _verbose > 2:
                click.secho(f"... will import path '{bookmark_path}'", fg="green")
        except ValueError as e:
            # if the file is missing, we can't resolve the bookmark
            click.secho(
                f"Skipping missing file '{pathstr}', cannot resolve bookmarks for missing files.",
                err=True,
                fg="red",
            )
    return referenced_files


def import_file_to_photos(filepath):
    """import a file into Photos"""
    import_files_to_photos([filepath])


def import_files_to_photos(filepaths):
    """import a file into Photos"""
    pl = PhotosLibrary()
    if _verbose > 2:
        click.secho("... doing import of ", fg="green")
        for filepath in filepaths:
            click.secho(f"... -- '{filepath}' ", fg="green")
    pl.import_photos(list(filepaths), skip_duplicate_check=True)


def _read_zfilesystem_bookmark_from_photos_database(
    photos_db_path, *, filter_null_bookmark_data=True
):
    """Dump the main useful contents of the ZFILESYSTEMBOOKMARK table.
    filter_null_bookmark_data is True by default, which skips records where zbookmarkdata is null.
    This returns a list of tuples, with primarykey, pathrel, and bookmarkdata.
    """
    (conn, c) = open_sqlite_db(photos_db_path)
    c.execute(
        "SELECT Z_PK, ZPATHRELATIVETOVOLUME, ZBOOKMARKDATA FROM ZFILESYSTEMBOOKMARK"
    )
    results = []
    for row in c:
        pk = row[0]
        pathstr = row[1]
        bookmark_data = row[2]
        if bookmark_data or filter_null_bookmark_data is False:
            results.append((pk, pathstr, bookmark_data))
    conn.close()
    return results


def _get_bookmark_data_by_path(db_path):
    results = _read_zfilesystem_bookmark_from_photos_database(db_path)
    bookmarks_by_path = {}
    for pk, pathstr, bookmarkdata in results:
        # resolve the bookmark data
        filepath = resolve_bookmark_path(bookmarkdata)
        bookmarks_by_path[filepath] = bookmarkdata
    return bookmarks_by_path


def update_bookmarks_in_photos_database(
    referenced_files, photos_db_path, import_db_path
):
    """Update bookmarks for referenced files in a Photos library database"""
    new_bookmarks = _get_bookmark_data_by_path(import_db_path)
    # update each bookmark in the database
    (conn, c) = open_sqlite_db(photos_db_path)
    updated_paths = set()
    for pk, filepath in referenced_files.items():
        if _verbose > 0:
            click.secho(
                f"Updating bookmark for {filepath} with primary key = {pk}", fg="green"
            )
        if filepath not in new_bookmarks:
            click.secho(
                f"File '{filepath}' is not in ZFILESYSTEMBOOKMARK", fg="red", err=True
            )
        else:
            bookmark_data = new_bookmarks[filepath]
            conn.execute(
                "UPDATE ZFILESYSTEMBOOKMARK SET ZBOOKMARKDATA = ? WHERE Z_PK = ?",
                (bytes(bookmark_data), pk),
            )
            updated_paths.add(filepath)
    if _verbose > 0:
        missing = set(referenced_files.values()).difference(updated_paths)
        for pathstr in missing:
            click.secho(f"File '{pathstr}' was not updated", fg="yellow", err=True)
        if not missing:
            click.secho("All files were updated")
    conn.commit()
    conn.close()


def get_previously_imported_filepaths(photos_db_path):
    results = _read_zfilesystem_bookmark_from_photos_database(photos_db_path)
    allpaths = set()
    for pk, pathstr, bookmark_data in results:
        fullpath = resolve_bookmark_path(bookmark_data)
        allpaths.add(fullpath)
    return allpaths


# from https://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
def grouper_it(n, iterable):
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


def filename_parts_from_filepath(filepath):
    """Apple Photos has many files that are really a group, e.g.
    IMG_2212.JPG, IMG_2212.MOV, IMG_2212.AAE, IMG_E2212.JPG IMG_O2212.JPG
    all should be imported as a group."""
    # The simplest algorithm is just to pick off the last 4 characters and use that...
    path, filename = os.path.split(filepath)
    basename, ext = os.path.splitext(filename)
    # remove the IMG_ prefix.
    last4 = basename[-4:]  # last four digits
    return (path, last4)


def group_filepaths(filepaths):
    """This takes a list of filepaths and returns all the groups."""
    keyfunc = filename_parts_from_filepath
    gsorted = sorted(filepaths, key=keyfunc)
    return [list(g) for k, g in itertools.groupby(gsorted, keyfunc)]


def _already_all_imported(group, imported_filepaths):
    """Test is all the filepaths in groups are already imported in
    imported_filepaths"""
    nimported = sum(fp in imported_filepaths for fp in group)
    return nimported == len(group)


def make_import_groups(filepaths, imported_filepaths):
    """Group files and find all the groups where at least one file isn't imported."""
    groups = group_filepaths(filepaths)
    check_group = lambda group: not _already_all_imported(group, imported_filepaths)
    return filter(check_group, groups)


def move_aae_file_if_it_exists(filepath, do_not_move_set=None):
    """Check if an AAE file exists for this file path, if so, we move it to an AAE.bak file,
    And return the pair of original AAE file path and moved file path. If the AAE file does not
    Exist, we return None."""

    # check upper and lowercase aae extensions
    # make sure file path exists
    test_exts = [".AAE", ".aae"]
    if os.path.exists(filepath):
        basename, ext = os.path.splitext(filepath)
        for aae_ext in test_exts:
            aaepath = basename + aae_ext
            if os.path.exists(aaepath):
                # make sure we aren't supposed to import this...
                if do_not_move_set is not None and aaepath in do_not_move_set:
                    if _verbose > 1:
                        click.secho(f"... keeping {aaepath} for import", fg="green")
                    return None
                newpath = f"{aaepath}.bak"
                if _verbose:
                    click.secho(
                        f"... moving {aaepath} to {newpath} for import", fg="green"
                    )
                os.rename(aaepath, newpath)

                return (aaepath, newpath)
    return None


def move_aae_files_back(moved_aae):
    for (original_file, moved_file) in moved_aae:
        if _verbose > 0:
            click.secho(f"... moving {moved_file} back to {original_file}", fg="green")
        os.rename(moved_file, original_file)


@click.command()
@click.argument("photos_library_path", type=click.Path(exists=True))
@click.option("-v", "--verbose", count=True)
@click.option("--restart", default=False)
@click.option("--groupsize", default=5)
@click.option("--move-aae", default=True)
@click.option("--max-imports", default=10000)
@click.option("--imports-before-pausing", default=250)
def main(
    photos_library_path,
    verbose,
    restart,
    groupsize,
    move_aae,
    max_imports,
    imports_before_pausing,
):
    """Repair photo bookmarks in a Photos sqlite database"""
    global _verbose
    _verbose = verbose

    if _verbose:
        print(f"Verbose mode is on, level={_verbose}")
    if _verbose:
        click.echo(f"  restart = {restart}")
        click.echo(f"  groupsize = {groupsize}")
        click.echo(f"  photos_library_path = {photos_library_path}")
        click.echo(f"  move_aae = {move_aae}")
        click.echo(f"  max_imports = {max_imports}")
        click.echo(f"  imports_before_pausing = {imports_before_pausing}")

    photos_db_path = pathlib.Path(photos_library_path) / "database/Photos.sqlite"
    if not photos_db_path.is_file():
        raise FileNotFoundError(f"Could not find Photos database at '{photos_db_path}'")
    photos_db_path = str(photos_db_path)

    click.confirm(
        "WARNING: this program will rewrite your Photos library database and could result in total data loss.\n"
        "Please ensure you have a backup and that the Photos app is not running!\n"
        "Do you want to proceed?",
        abort=True,
    )

    click.confirm(
        "Please open Photos and uncheck the box 'Importing: Copy items to the Photos library' in Photos Preferences.\n"
        "Type 'y' when you have done this.",
        abort=True,
    )

    click.confirm(
        "Please quit Photos.\n" "Type 'y' when you have done this.",
        abort=True,
    )

    while photos_is_running() and restart == False:
        click.secho("Photos is still running, please quit it", fg="red", err=True)
        click.confirm(
            "Please quit Photos.\n" "Type 'y' when you have done this.",
            abort=True,
        )

    if restart == False:
        click.echo("Creating a temporary working Photos library.")
        temp_library_path = copy_temporary_photos_library()
        click.echo(f"Created temporary Photos library at: {temp_library_path}")
    else:
        temp_library_path = str(get_temp_photos_library_dir())

    click.confirm(
        "Please open Photos while holding down the Option key then select the temporary working library.\n"
        "Type 'y' when you have done this.",
        abort=True,
    )

    if not photos_is_running():
        click.secho("Photos is not running, please open it", fg="red", err=True)
        click.confirm(
            "Please open Photos while holding down the Option key then select the temporary working library.\n"
            "Type 'y' when you have done this.",
            abort=True,
        )

    while not verify_temp_library_signature():
        click.secho(
            "Photos library missing sentinel value--does not appear to be temporary library. "
            "Are you sure you opened the right library?",
            err=True,
            fg="red",
        )
        click.confirm(
            "Please open Photos while holding down the Option key then select the temporary working library.\n"
            "Type 'y' when you have done this.",
            abort=True,
        )

    click.echo("Reading data for referenced files from target library")
    referenced_files = read_file_locations_from_photos_database(photos_db_path)

    # read the bookmarks that have already been imported (this is likely to be NONE)
    # the first time it is run.
    temp_db_path = pathlib.Path(temp_library_path) / "database/Photos.sqlite"
    imported_bookmarks = get_previously_imported_filepaths(temp_db_path)
    if restart == False:
        assert len(imported_bookmarks) == 0
    else:
        click.echo(
            f"Found '{len(imported_bookmarks)}' already imported from previous run"
        )

    to_import_set = set(referenced_files.values())
    import_groups = list(
        make_import_groups(referenced_files.values(), imported_bookmarks)
    )

    ntried = 0
    click.echo("Importing photos into temporary working library")
    for filepath_groups in grouper_it(groupsize, import_groups):
        filepaths = [fp for fplist in filepath_groups for fp in fplist]

        to_import = []
        moved_aae = []
        for filepath in filepaths:
            click.echo(f"Processing file {filepath}")
            if not os.path.exists(filepath):
                click.secho(
                    f"Skipping missing file '{filepath}', cannot rewrite bookmarks for missing files.",
                    err=True,
                    fg="red",
                )
                continue
            if filepath in imported_bookmarks:
                if _verbose > 1:
                    click.secho(
                        f"... used previously imported '{filepath}'", fg="green"
                    )
            else:
                to_import.append(filepath)

            if move_aae:
                aaefile = move_aae_file_if_it_exists(
                    filepath, do_not_move_set=to_import_set
                )
                if aaefile is not None:
                    moved_aae.append(aaefile)

        if to_import:
            import_files_to_photos(to_import)
            time.sleep(0.25)
            ntried += 1
            if moved_aae:
                move_aae_files_back(moved_aae)
            if ntried % imports_before_pausing == 0:
                click.echo(
                    f"Pausing after {imports_before_pausing} imports (total imports = {ntried})"
                )
                pl = PhotosLibrary()
                pl.quit()
                time.sleep(SLEEP_TIME_AFTER_QUIT)
                pl.activate()
                time.sleep(SLEEP_TIME_AFTER_ACTIVATE)
            if ntried >= max_imports:
                click.echo("Stopping after {max_imports} imports")
                sys.exit(1)

    click.confirm(
        "Please quit Photos.\n" "Type 'y' when you have done this.",
        abort=True,
    )
    while photos_is_running():
        click.secho("Photos is still running, please quit it", fg="red", err=True)
        click.confirm(
            "Please quit Photos.\n" "Type 'y' when you have done this.",
            abort=True,
        )

    click.echo("Rewriting bookmarks in target library")
    update_bookmarks_in_photos_database(referenced_files, photos_db_path, temp_db_path)

    click.confirm(
        f"Please open Photos while holding down the Option key then select your target library: {photos_library_path}\n"
        "Type 'y' when you have done this.",
        abort=True,
    )

    while not photos_is_running():
        click.secho("Photos is not running, please open it", fg="red", err=True)
        click.confirm(
            f"Please open Photos while holding down the Option key then select your target library: {photos_library_path}\n"
            "Type 'y' when you have done this.",
            abort=True,
        )

    while verify_temp_library_signature():
        click.secho(
            "It appears the temporary Photos library is still open. Are you sure you opened the right library?",
            err=True,
            fg="red",
        )
        click.confirm(
            f"Please open Photos while holding down the Option key then select your target library: {photos_library_path}\n"
            "Type 'y' when you have done this.",
            abort=True,
        )

    click.echo(
        "If you want newly imported files copied into the Photos library, be sure to check the following box in Photos preferences:\n"
        "'Importing: Copy items to the Photos library'"
    )
    click.echo(
        f"You may now delete the temporary Photos library by dragging it to the Trash in Finder: {temp_library_path}"
    )
    click.echo("Done.")


if __name__ == "__main__":
    main()
