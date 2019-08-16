"""
python -m tracking.chmod the_dir

recursively change all file permissions to 
rw-r--r--

and directory permissions to
drwxr-xr-x
"""
import os
import stat
from pathlib import Path

def scantree(path):
    """Recursively yield DirEntry objects for given directory."""
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            yield from scantree(entry.path)
        else:
            yield entry

def chmod(path):            
    for direntry in scantree(path):
        full_name=Path(direntry.path)
        if full_name.is_file():
            try:
                fileStats=os.stat(full_name)
                full_name.chmod(fileStats.st_mode | stat.S_IROTH | stat.S_IRGRP)
            except (FileNotFoundError,PermissionError):
                print("permission denied for ",full_name)
        else:
            print("working on directory: ",full_name)
            fileStats=full_name.stat()
            try:
                full_name.chmod(fileStats.st_mode | stat.S_IXOTH |  stat.S_IROTH | stat.S_IXGRP |  stat.S_IRGRP)
            except PermissionError:
                print("permission denied for ",full_name)

if __name__ == "__main__":
    import argparse
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('path',nargs='?',type=str,help='path to base of tree with default .', default='.')
    args=parser.parse_args()
    chmod(args.path)
    
