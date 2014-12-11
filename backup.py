#
# TODO:
# 1. For father backup, check if there is single grandfather backup,
#    before going to previous backups.
#
from collections import namedtuple
from pathlib import Path
from pathlib import PurePath

import click
import datetime
import itertools
import logging
import re
import subprocess
import tempfile

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("backup")


ETC = '/etc/backup'
DESTDIR = '/backup'


Backup = namedtuple("Backup", ['timestamp'])
BackupLabel = namedtuple("BackupLabel", ["part1", "part2"])
Node = namedtuple("Node", ["value", "children"])


# Name of the subdiretory where backup data is stored.
data_dir_name = '_data'

# For encoding datetime into filesystem path.
ts_format = '%Y-%m-%d_%H:%M:%S'

# For globbing directories where backups are stored.
dd = '[0-9]' * 2
glob_format = '{}{}-{}-{}_{}:{}:{}'.format(*([dd] * 7))


class Config(object):
    # TODO: Paths
    def __init__(self, root, workdir, debug=False):
        self.root = Path(root)
        self.workdir = Path(workdir)
        self.debug = debug
        # TODO
        self.config_dir = Path(ETC)


def make_dar_create_command(config_path, basename_path,
                            reference_basename_path):
    cmd = ["dar",
           "-N", # skip /etc/darrc
           "-c", str(basename_path),
           "--batch", str(config_path),
           ]
    if reference_basename_path is not None:
        cmd.extend(["--ref", str(reference_basename_path)])
    return cmd


def make_dar_test_command(config):
    reference_basename = None
    return ["dar",
            "--batch", config.config_file,
            reference_basename
            ]


def run_dar(command):
    log.info(repr(command))
    subprocess.check_call(command)
    log.info("Done")


def backup_label_to_path(label):
    return PurePath(label.part1) / label.part2


def backup_to_path(backups):
    """Converts backup to a relative pure path.
    Args:
        backups - list of backups (from leaf to root).
    Returns:
        Filesystem path.
    """
    return PurePath('').joinpath(*(backup.timestamp.strftime(ts_format)
                                 for backup in reversed(backups)))


def path_to_backup(path, parents):
    """Converts path to backup.
    Args:
        path - filesystem relative path (just directory),
        parent - list of parent backups (from leaf to root).
    Returns:
        List of backups from leaf to root.
    """
    timestamp = datetime.datetime.strptime(str(path), ts_format)
    return [Backup(timestamp)] + (parents or [])


def list_backups(root_path):
    """Converts filesystem directories to a sequences of backups.
    Args:
        root_path - Root filesystem paths, under whicht the backup
                    directories are located.
    Returns:
        List of backups. Each backup is a sequence of Backup objects,
        from leaf to root. Sorted from latest to earliest.
    """
    def one_level(path, parent, children):
        root = path.resolve()
        for path in root.glob(glob_format):
            if path.is_dir() and (path / data_dir_name).is_dir():
                date_path = PurePath(path.relative_to(root))
                backup = path_to_backup(date_path, parent)
                node = Node(backup, [])
                children.append(node)
                one_level(path, backup, node.children)

    nodes = []
    one_level(root_path, None, nodes)
    ret = []

    def invert(ret, nodes):
        for node in nodes:
            ret.append(node.value)
            invert(ret, node.children)
    invert(ret, nodes)

    keys = [[backup.timestamp for backup in backups] for backups in ret]
    ret = [pair[1] for pair in sorted(zip(keys, ret), reverse=True)]
    return ret


def dir_size(dir_path):
    """Returns total size of all files in the given directory path.
    Only one level files are checked. Symlinks are dereferenced.
    Args:
        dir_path - Directory path
    Returns:
        Number of bytes.
    """
    def fsize(path):
        target = path.resolve()
        if target.is_file():
            return target.stat().st_size
        else:
            return 0
    return sum(fsize(child) for child in dir_path.iterdir())


def latest_backup(backups, level):
    """Returns the latest backup with the given level.
    Args:
        backups - List of backups (lists from leaf to root),
        level - number of levels in backup (length of list).
    Return:
        backup (list from leaf to root) or None
    """
    tail = itertools.dropwhile(lambda x: len(x) != level, backups)
    try:
        return next(tail)
    except StopIteration:
        return None


def validate_backup_label(ctx, param, value):
    if not param.required and value is None:
        return None
    path = PurePath(value)
    if len(path.parts) != 2:
        raise click.BadParameter('label needs to be in format text1/text2')
    if not all(re.match("[0-9a-zA-Z_-]+", part) for part in path.parts):
        raise click.BadParameter('label contains invalid characters')
    return BackupLabel(*path.parts)


@click.group()
@click.option('--root',
              type=click.Path(exists=True, dir_okay=True, file_okay=False),
              required=False,
              help="root directory, where backups are stored")
@click.option('--workdir',
              type=click.Path(exists=True, dir_okay=True, file_okay=False),
              help='work directory'
              )
@click.option('--debug', help="debug", is_flag=True, default=False)
@click.pass_context
def main(ctx, root, workdir, debug):
    root = Path(root or DESTDIR)
    default_workdir_name = '.workdir'
    workdir = Path(workdir or Path(root) / default_workdir_name)
    # TODO: Disallow unsafe characters in param 'workdir'.
    assert str(workdir.relative_to(root)) == default_workdir_name
    ctx.obj = Config(root=root, workdir=workdir, debug=debug)


@main.command(name="make")
@click.option('--label', help="backup label",
              callback=validate_backup_label, required=True)
@click.option('--level', type=click.Choice(['g', 'f', 's']),
              help="backup level ('g'randfather, 'f'ather, 's'on)",
              default='s', show_default=True)
@click.option('--dry-run', is_flag=True, default=False,
              help='do nothing, just print commands to be executed')
@click.pass_obj
def cmd_make(config, label, level, dry_run):
    """Creates new backup."""
    if level == 'g':
        click.echo("{} backup.".format(click.style("Normal", fg="green")))
    else:
        click.echo("Differential backup.")

    def ff(all_backups, level):
        parent = latest_backup(all_backups, level)
        if parent is None:
            click.echo("No backups at level {} found.".format(level))
        return parent

    backup = [Backup(datetime.datetime.now())]
    level_num = 'gfs'.index(level)
    root = config.root / label.part1 / label.part2

    if level_num > 0:
        all_backups = list_backups(root)
        parent_g = ff(all_backups, 1)
        parent_f = ff(all_backups, 2)
        if parent_g is None:
            click.echo("No backups found - will make a regular backup.")
        else:
            if parent_f is None:
                parent = parent_g
            elif parent_f[0].timestamp >= parent_g[0].timestamp:
                parent = parent_f
            else:
                parent = parent_g
            backup.extend(parent)

    click.echo("Create backup: {}".format(backup_to_path(backup)))

    target_path = root / backup_to_path(backup) / data_dir_name

    if len(backup) > 1:
        basename = backup_to_path(backup[1:2])
        backup_path = backup_to_path(backup[1:])
        reference_basename_path = root / backup_path / data_dir_name / basename
    else:
        reference_basename_path = None

    config_file_name = "{}-{}.dcf".format(label.part1, label.part2)
    config_path = config.config_dir / config_file_name

    # TODO
    assert config.config_dir.is_dir()
    backup_dir = Path(tempfile.mkdtemp(dir=str(config.workdir)))
    basename_path = backup_dir / backup_to_path(backup[0:1])

    command = make_dar_create_command(
        config_path=config_path,
        basename_path=basename_path,
        reference_basename_path=reference_basename_path
        )
    try:
        if dry_run:
            log.info(command)
        else:
            run_dar(command)
    finally:
        if not dry_run:
            target_path.parent.mkdir(parents=True)
            backup_dir.rename(target_path)


@main.command(name="list")
@click.option('--tree', is_flag=True, default=False,
              help='list backup hierarchy as a tree')
@click.option('--label', help="backup label",
              callback=validate_backup_label,
              required=False)
@click.pass_obj
def cmd_list(config, tree, label):
    """Lists backups."""
    if tree:
        # TODO
        pass
    else:
        if label:
            labels = [label]
        else:
            labels = get_backup_labels(config.root)

        def ff(path):
            return click.format_filename(str(path)) if path else None
        print("Root:    {}".format(ff(config.root)))
        print("Workdir: {}".format(ff(config.workdir)))
        print("Backups:")

        for label in labels:
            print("{}:".format(backup_label_to_path(label)))
            full_path = config.root / backup_label_to_path(label)
            for backup in reversed(list_backups(full_path)):
                backup_path = backup_to_path(backup)
                backup_size = dir_size(full_path / backup_path / data_dir_name)
                backup_size_str = human_size(backup_size)
                print(" - {} ({})".format(backup_path, backup_size_str))


def human_size(num):
    units = 'KMGT'

    def _fmt(num, unit):
        return '{:.1f}{}'.format(num, unit)

    if num < 1024:
        return str(num)
    else:
        for unit in units:
            num = num / 1024.0
            if num < 1024:
                return _fmt(num, unit)
    return _fmt(num, units[-1])


def get_backup_labels(root):
    def subdirs(path):
        for subpath in path.iterdir():
            if subpath.is_dir() and not subpath.name.startswith('.'):
                yield subpath

    root = root.resolve()

    for path in subdirs(root):
        for subpath in subdirs(path):
            yield BackupLabel(path.name, subpath.name)
