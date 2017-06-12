import os
import luigi
import subprocess
import gzip
from collections import defaultdict


###############################################################################
#                          dict/list handling                                 #
###############################################################################

def structure_apply(f, struct):
    if isinstance(struct, dict):
        return {k: f(v) for k, v in struct.items()}
    elif isinstance(struct, list):
        return [f(x) for x in struct]
    else:
        return f(struct)


def value_it(struct):
    return struct.values() if isinstance(struct, dict) else list(struct)


###############################################################################
#                               File handling                                  #
###############################################################################

def get_ext(path):
    '''Split path into base and extention, gracefully handling compressed extensions eg .gz'''
    base, ext1 = os.path.splitext(path)
    if ext1 == '.gz':
        base, ext2 = os.path.splitext(base)
        return base, ext2 + ext1
    else:
        return base, ext1
###############################################################################
#                         Testing file emptiness                              #
###############################################################################


def isNeGz(fname):
    with gzip.open(fname, 'rb') as f:
        data = f.read(1)
    return len(data) > 0


def isNePlain(fname):
    return os.path.getsize(fname) > 0


def isNeBam(fname):
    r = subprocess.run("set -o pipefail; source samtools-0.1.19 && samtools view {0} | head -c1 ".format(
        fname), shell=True, stdout=subprocess.PIPE)

    if r.returncode == 1:
        # samtools will return 1 if fname is not a valid bam
        # Check specifically for 1 rather than !=0 as if
        # The file is bam and non-empty head causes the retcode to be SIGPIPE 141
        raise subprocess.CalledProcessError(r.returncode, str(r.args))

    return len(r.stdout) > 0


extensions_dispath = defaultdict(lambda: isNePlain, {'bam': isNeBam,
                                                     'gz': isNeGz})


def is_not_empty(fname):

    try:
        # Try dispatching to handler determmined by file extension
        _, ext = fname.rsplit('.', 1)
        return extensions_dispath[ext](fname)

    except ValueError:
        try:
            return isNeBam(fname)
        except subprocess.CalledProcessError:
            try:
                return isNeGz(fname)
            except OSError:
                return os.path.getsize(fname) > 0


class CheckTargetNonEmpty(object):
    '''This is mixin class that can be added to a luigi task to cause it to fail if the produced output is exists but is empty
        Handles checking compressed files which can have nonzero size but still be empty'''

    def complete(self):
        outputs = luigi.task.flatten(self.output())
        return super().complete() and all(map(is_not_empty, [x.path for x in outputs]))
