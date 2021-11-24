# Repair Apple Photos Bookmarks

Work in progress to repair file location bookmarks in Apple Photos. 

## Background

Starting in macOS 10.15/Catalina, photos imported into the Apple Photos app and not copied into the library (e.g. referenced photos) are tied to a specific volume on a specific computer. If you move the photos to a different volume or computer, even with the same name, Photos will be unable to open the original files and will produce a "Missing file" dialog.  This is caused because of macOS Sandbox security features and the fact that Photos now stores locations to photos using security-scoped file system bookmarks instead by path. This script will repair your photos library by updating the bookmarks.

This is a work in progress and it's a bit of hack.  It requires a few manual steps to get it working (the script will prompt you).

## How to use

pip install -r requirements.txt
python3 repair_photos_bookmarks.py
follow the prompts

## Additional Information

For more information, see this [discussion](https://github.com/RhetTbull/osxphotos/discussions/319)
