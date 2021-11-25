"""Repair 'Missing File' errors in Photos caused by moving referenced files to a different drive"""

import os
import pathlib
import platform
import sqlite3
import time
import urllib

import click
import CoreFoundation
from Foundation import kCFAllocatorDefault
from photoscript import PhotosLibrary
from photoscript.utils import ditto

TEMPLATE_DIRECTORY = "template_libraries"
TEMPLATE_LIBRARY = {
    (10, 15): "osxphotos_temporary_working_library_catalina.photoslibrary"
}


def get_os_version():
    import platform

    # returns tuple containing OS version
    # e.g. 10.13.6 = (10, 13, 6)
    version = platform.mac_ver()[0].split(".")
    if len(version) == 2:
        (ver, major) = version
        minor = 0
    elif len(version) == 3:
        (ver, major, minor) = version
    else:
        raise (
            ValueError(
                f"Could not parse version string: {platform.mac_ver()} {version}"
            )
        )
    return (int(ver), int(major), int(minor))


def copy_temporary_photos_library():
    """copy the template library and open Photos, returns path to copied library"""
    ver, major, minor = get_os_version()
    if ver == 10:
        template_library = TEMPLATE_LIBRARY.get((ver, major))
    else:
        # MacOS versions after Catalina are 11, 12, etc. not 10.X
        template_library = TEMPLATE_LIBRARY.get((ver))
    if not template_library:
        raise ValueError(f"No template library for version {ver}.{major}.{minor}")

    src = pathlib.Path(TEMPLATE_DIRECTORY) / template_library

    # is picture folder always here independent of locale or language?
    picture_folder = pathlib.Path("~/Pictures").expanduser()
    if not picture_folder.is_dir():
        raise FileNotFoundError(f"Invalid picture folder: '{picture_folder}'")

    dest = picture_folder / template_library
    ditto(src, dest)

    return str(dest)


def open_sqlite_db(fname: str):
    """Open sqlite database and return connection to the database"""
    try:
        conn = sqlite3.connect(f"{fname}")
        c = conn.cursor()
    except sqlite3.Error as e:
        raise OSError(f"Error opening {fname}: {e}")
    return (conn, c)


def resolve_cfdata_bookmark(bookmark: bytes) -> str:
    """Resolve a bookmark stored as a serialized CFData object into a path str"""

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

    return os.path.normpath(urllib.parse.unquote(urllib.parse.urlparse(urlstr).path))


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
        if bookmark_data:
            try:
                bookmark_path = resolve_cfdata_bookmark(bookmark_data)
                referenced_files[pk] = bookmark_path
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
    pl.import_photos([filepath], skip_duplicate_check=True)


def read_bookmarks_from_photos_database(photos_db_path):
    """Read bookmarks for referenced files from a Photos library database"""
    (conn, c) = open_sqlite_db(photos_db_path)
    c.execute(
        "SELECT Z_PK, ZPATHRELATIVETOVOLUME, ZBOOKMARKDATA FROM ZFILESYSTEMBOOKMARK"
    )
    bookmarks = {}
    for row in c:
        pk = row[0]
        pathstr = row[1]
        bookmark_data = row[2]
        if bookmark_data:
            try:
                bookmark_path = resolve_cfdata_bookmark(bookmark_data)
                bookmarks[pathstr] = bookmark_data
            except ValueError as e:
                # if the file is missing, we can't resolve the bookmark
                click.secho(
                    f"Skipping missing file '{pathstr}', cannot read bookmarks for missing files.",
                    err=True,
                    fg="red",
                )
    conn.close()
    return bookmarks


def update_bookmarks_in_photos_database(photos_db_path, bookmarks):
    """Update bookmarks for referenced files in a Photos library database"""
    # update each bookmark in the database
    (conn, c) = open_sqlite_db(photos_db_path)
    for pathstr, bookmark_data in bookmarks.items():
        click.echo(f"Updating bookmark for {pathstr}")
        conn.execute(
            "UPDATE ZFILESYSTEMBOOKMARK SET ZBOOKMARKDATA = ? WHERE ZPATHRELATIVETOVOLUME = ?",
            (bytes(bookmark_data), pathstr),
        )
    conn.commit()
    conn.close()


@click.command()
@click.argument("photos_library_path", type=click.Path(exists=True))
def main(photos_library_path):
    """Repair photo bookmarks in a Photos sqlite database"""

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

    click.echo("Creating a temporary working Photos library.")

    temp_library_path = copy_temporary_photos_library()
    click.echo(f"Created temporary Photos library at {temp_library_path}")

    click.confirm(
        "Please open Photos while holding down the Option key then select the temporary working library.\n"
        "Type 'y' when you have done this.",
        abort=True,
    )

    # TODO: The template library should have a uniquely named album so we can confirm it's the one that's actually opened

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
        import_file_to_photos(filepath)

    click.confirm(
        "Please quit Photos.\n" "Type 'y' when you have done this.",
        abort=True,
    )
    temp_db_path = pathlib.Path(temp_library_path) / "database/Photos.sqlite"
    bookmarks = read_bookmarks_from_photos_database(temp_db_path)

    click.echo("Rewriting bookmarks in target library")
    update_bookmarks_in_photos_database(photos_db_path, bookmarks)

    click.confirm(
        f"Please open Photos while holding down the Option key then select your target library ({photos_library_path}).\n"
        "Type 'y' when you have done this.",
        abort=True,
    )
    click.echo("Done.")


if __name__ == "__main__":
    main()
