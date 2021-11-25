# Repair Apple Photos Bookmarks

Work in progress to repair file location bookmarks in Apple Photos. 

## Background

Starting in macOS 10.15/Catalina, photos imported into the Apple Photos app and not copied into the library (e.g. referenced photos) are tied to a specific volume on a specific computer. If you move the photos to a different volume or computer, even with the same name, Photos will be unable to open the original files and will produce a "Missing file" dialog.  This is caused because of macOS Sandbox security features and the fact that Photos now stores locations to photos using security-scoped file system bookmarks instead by path. This script will repair your photos library by updating the bookmarks.

This is a work in progress and it's a bit of hack.  It requires a few manual steps to get it working (the script will prompt you).

## How to use

First, move your Photos library to the new volume or the new computer.  Note: the absolute path to the photos must remain the same.  For example, if your photos were located on an external drive named "Fotos" and in a folder named "MyFotos" the path would `/Volumes/Fotos/MyFotos`, the new volume must also be named "Fotos" and the folder must be named "MyFotos". 

- pip install -r requirements.txt
- python3 repair_photos_bookmarks.py PATH_TO_PHOTOS_LIBRARY
- Follow the prompts

## Contributors

Thanks to [@dgleich](https://github.com/dgleich) for the idea behind this project and for contributing significant research, testing, and code!  This was a joint effort.

## TODO

- [ ] Add option to change the absolute path to the photos
- [ ] Incorporate as an option to [osxphotos](https://github.com/RhetTbull/osxphotos)

## Additional Information

For more information, see this [discussion](https://github.com/RhetTbull/osxphotos/discussions/319)
