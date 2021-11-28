"""Repair 'Missing File' errors in Photos caused by moving referenced files to a different drive"""

import os
import pathlib
import sqlite3
import urllib

import click
import CoreFoundation
import objc
import psutil
from Foundation import kCFAllocatorDefault
from mac_alias import Bookmark, kBookmarkPath
from photoscript import PhotosLibrary
from photoscript.utils import ditto

_verbose = 0

TEMPLATE_DIRECTORY = "template_libraries"
TEMPLATE_LIBRARY = "osxphotos_temporary_working_library.photoslibrary"
TEMP_LIBRARY_SENTINEL_ALBUM = "ZZZ_OSXPHOTOS_SENTINEL_ZZZ"


def copy_temporary_photos_library():
    """copy the template library and open Photos, returns path to copied library"""
    src = pathlib.Path(TEMPLATE_DIRECTORY) / TEMPLATE_LIBRARY

    # is picture folder always here independent of locale or language?
    picture_folder = pathlib.Path("~/Pictures").expanduser()
    if not picture_folder.is_dir():
        raise FileNotFoundError(f"Invalid picture folder: '{picture_folder}'")

    dest = picture_folder / TEMPLATE_LIBRARY
    ditto(src, dest)

    return str(dest)


def verify_temp_library_signature():
    """Verify that the loaded library is actually the temporary working library"""
    return PhotosLibrary().album(TEMP_LIBRARY_SENTINEL_ALBUM) is not None


def photos_is_running():
    """Check if Photos is running"""
    return any(p.name() == "Photos" for p in psutil.process_iter())


def open_sqlite_db(fname: str):
    """Open sqlite database and return connection to the database"""
    try:
        conn = sqlite3.connect(f"{fname}")
        c = conn.cursor()
    except sqlite3.Error as e:
        raise OSError(f"Error opening {fname}: {e}")
    return (conn, c)


def resolve_bookmark_path(bookmark_data: bytes) -> str:
    """Get the path from a CFURL file bookmark
    This works without calling CFURLCreateByResolvingBookmarkData
    which fails if the target file does not exist
    """
    try:
        bookmark = Bookmark.from_bytes(bookmark_data)
    except Exception as e:
        raise ValueError(f"Invalid bookmark: {e}")
    path_components = bookmark.get(kBookmarkPath, None)
    if not path_components:
        return None
    return "/" + os.path.join(*path_components)


def resolve_cfdata_bookmark(bookmark: bytes) -> str:
    """Resolve a bookmark stored as a serialized CFData object into a path str"""

    with objc.autorelease_pool():
        # use CFURLCreateByResolvingBookmarkData to de-serialize bookmark data into a CFURLRef
        url = CoreFoundation.CFURLCreateByResolvingBookmarkData(
            kCFAllocatorDefault, bookmark, 0, None, None, None, None
        )

        # the CFURLRef we got is a sruct that python treats as an array
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

    (conn, c) = open_sqlite_db(photos_db_path)
    c.execute(
        "SELECT Z_PK, ZPATHRELATIVETOVOLUME, ZBOOKMARKDATA FROM ZFILESYSTEMBOOKMARK"
    )

    # read all the bookmarks
    # TODO: Do we really need to resolve the bookmarks or can we construct the path from the ZFILESYSTEMBOOKMARK.ZPATHRELATIVETOVOLUME and ZFILESYSTEMVOLUME.ZNAME fields?
    referenced_files = {}
    for row in c:
        pk = row[0]
        pathstr = row[1]
        bookmark_data = row[2]
        try:
            bookmark_path = resolve_bookmark_path(bookmark_data)
            referenced_files[pk] = bookmark_path
            if _verbose > 1:
                click.secho(f"... will import path '{bookmark_path}'", fg="green")
        except ValueError as e:
            # if the file is missing, we can't resolve the bookmark
            click.secho(
                f"Skipping missing file '{pathstr}', cannot resolve bookmarks for missing files.",
                err=True,
                fg="red",
            )
    conn.close()
    return referenced_files


def import_file_to_photos(filepath):
    """import a file into Photos"""
    pl = PhotosLibrary()
    if _verbose > 2:
        click.secho(f"... doing import of '{filepath}'", fg="green")
    pl.import_photos([filepath], skip_duplicate_check=True)


def read_bookmarks_from_photos_database(photos_db_path):
    """Read bookmarks for referenced files from a Photos library database"""
    (conn, c) = open_sqlite_db(photos_db_path)
    c.execute(
        "SELECT Z_PK, ZPATHRELATIVETOVOLUME, ZBOOKMARKDATA FROM ZFILESYSTEMBOOKMARK"
    )
    bookmarks = {}
    for row in c:
        pathstr = row[1]
        bookmark_data = row[2]
        if bookmark_data:
            bookmarks[pathstr] = bookmark_data
    conn.close()
    return bookmarks


def check_if_pathstr_in_filesystem_bookmarks(c, pathstr):
    c.execute(
        "SELECT COUNT(*) FROM ZFILESYSTEMBOOKMARK WHERE ZPATHRELATIVETOVOLUME = ?",
        (pathstr,),
    )
    for row in c:
        if row[0] == 0:
            return False
        elif row[0] == 1:
            return True
        else:
            click.secho(
                f"File '{pathstr}' has multiple entries in ZFILESYSTEMBOOKMARK table",
                err=True,
                fg="yellow",
            )
            return True


def get_pathstrs_in_filesystembookmarks(c):
    c.execute("SELECT ZPATHRELATIVETOVOLUME FROM ZFILESYSTEMBOOKMARK")
    return {row[0] for row in c}


def update_bookmarks_in_photos_database(photos_db_path, bookmarks):
    """Update bookmarks for referenced files in a Photos library database"""
    # update each bookmark in the database
    (conn, c) = open_sqlite_db(photos_db_path)
    pathstrs = get_pathstrs_in_filesystembookmarks(c)
    updated_pathstrs = set()
    for pathstr, bookmark_data in bookmarks.items():
        if _verbose > 0:
            click.secho(f"Updating bookmark for {pathstr}", fg="green")
        if check_if_pathstr_in_filesystem_bookmarks(c, pathstr) == False:
            click.secho(
                f"File '{pathstr}' is not in ZFILESYSTEMBOOKMARK", fg="red", err=True
            )
        conn.execute(
            "UPDATE ZFILESYSTEMBOOKMARK SET ZBOOKMARKDATA = ? WHERE ZPATHRELATIVETOVOLUME = ?",
            (bytes(bookmark_data), pathstr),
        )
        updated_pathstrs.add(pathstr)
    if _verbose > 0:
        missing = pathstrs.difference(updated_pathstrs)
        for pathstr in missing:
            click.secho(f"File '{pathstr}' was not updated", fg="yellow", err=True)
        if len(pathstrs) == 0:
            click.secho("All files were updated", fg="green")
    conn.commit()
    conn.close()


@click.command()
@click.argument("photos_library_path", type=click.Path(exists=True))
@click.option("-v", "--verbose", count=True)
@click.option("--debug-skip-import/--no-debug-skip-import", default=False)
def main(photos_library_path, verbose, debug_skip_import):
    """Repair photo bookmarks in a Photos sqlite database"""
    global _verbose
    _verbose = verbose
    if _verbose:
        print(f"Verbose mode is on, level={_verbose}")

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

    while photos_is_running():
        click.secho("Photos is still running, please quit it", fg="red", err=True)
        click.confirm(
            "Please quit Photos.\n" "Type 'y' when you have done this.",
            abort=True,
        )

    click.echo("Creating a temporary working Photos library.")
    temp_library_path = copy_temporary_photos_library()
    click.echo(f"Created temporary Photos library at: {temp_library_path}")

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

    click.echo("Importing photos into temporary working library")
    for filepath in referenced_files.values():
        click.echo(f"Processing file {filepath}")
        if not os.path.exists(filepath):
            click.secho(
                f"Skipping missing file '{filepath}', cannot rewrite bookmarks for missing files.",
                err=True,
                fg="red",
            )
            continue
        import_file_to_photos(filepath)

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

    temp_db_path = pathlib.Path(temp_library_path) / "database/Photos.sqlite"
    bookmarks = read_bookmarks_from_photos_database(temp_db_path)

    click.echo("Rewriting bookmarks in target library")
    update_bookmarks_in_photos_database(photos_db_path, bookmarks)

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
