import os,errno
def silent_remove(filename):
    """remove a directory without failure
       if directory doesn't exist
    """
    print(("{:s} will be destroyed if it exists".format(filename)))
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            raise # re-raise exception if a different error occured

def new_dir(the_dir):
    """create a directory without failure if
       directory already exists
    """
    try:
        os.makedirs(the_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise  #re-raise if different error occured
    return None
