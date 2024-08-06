#!/usr/bin/python

# Copyright: (c) 2022, Dee'Kej <devel@deekej.io>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

DOCUMENTATION = r'''
---
module: image.extract

short_description: Extracts the contents of given container image

description:
    - This module extracts the specified path from the given container image - without the need to have M(docker) or M(podman) installed...

options:
    image:
        description:
          - Path to the container image that should be extracted.
          - Both compressed (M(tar.gz)) and uncompressed (M(tar)) images are supported.
        required: true
        default: null
        type: string

    src:
        description:
          - Path to the file / folder inside the container that should be extracted.
          - Supports basic globbing for folders in the form of M(src/*), which allows extraction of all the files & subfolders of the specified M(src) path.
          - When the globbing is used, then the M(force) option is automatically assumed for the contents of that given folder. I.e. the contents are extracted every time.
          - This option is mandatory - unless the M(paths) option below is used instead.
        required: true
        default: null
        type: path

    dest:
        description:
          - Path where the extracted file / folder should be placed.
          - If the path is relative, then the M(chdir) option must be used (see below).
          - If the M(dest) is omitted, then the file / folder is extracted to the current working directory.
        required: false
        default: null
        type: path

    owner:
        description:
          - Name of the owner for all the extracted files / folders.
          - Set by UNIX's M(chown) utility.
        required: false
        default: null
        type: string

    group:
        description:
          - Name of the group for all the extracted files / folders.
          - Set by UNIX's M(chown) utility.
        required: false
        default: null
        type: string

    paths:
        description:
          - List of of multiple paths (files/folders) to be extracted from the container image.
          - Each element of the list is a YAML dictionary, which accepts these options:
            M(src) | M(dest) | M(owner) | M(group)
          - See the same options above for their description.
          - If this option is used, then at least M(src) must be specified for each member of this list.
        required: false
        default: null
        type: list

    chdir:
        description:
          - Directory to change into before opening the image / extracting the paths.
          - This option is mandatory if any relative paths are used.
        requied: false
        default: null
        type: path

    force:
        description:
          - Runs the container image extraction even if previously extracted files / folders exists.
        required: false
        default: false
        type: boolean

author:
    - Dee'Kej (@deekej)
'''

EXAMPLES = r'''
- name: Extract file from container image
  image.extract:
    image:  ~/rhel-9-ubi.tar
    src:    /usr/lib/os-release
    dest:   ~/rhel9-os-release
    owner:  ansible-automation
    group:  ansible-automation

- name: Extract file from container image using relative paths
  image.extract:
    image:  rhel-9-ubi.tar
    src:    /usr/lib/os-release
    chdir:  /home/deekej

- name: Extract contents of a whole folder from container image
  image.extract:
    image:  rhel-9-ubi.tar
    src:    /usr/lib/*
    dest:   rhel9/lib
    chdir:  /home/deekej
  # NOTE: The destination directory must exist already!

- name: Extract multiple files from container image:
  image.extract:
    image:  rhel-9-ubi.tar
    chdir:  /home/deekej
    force:  true
    paths:
      # Extracting files to system requires specifying 'dest':
      - src:    /usr/bin/*
        dest:   /usr/local/bin
        owner:  root
        group:  root
      - src:    /usr/lib/*
        dest:   rhel9/lib
        owner:  deekej
        group:  deekej
      - src:    /usr/lib/os-release
  become: true
  become_user: root
'''

# =====================================================================

import atexit
import copy
import grp
import json
import os
import pwd
import tarfile

from os import chown

from os.path import abspath
from os.path import basename
from os.path import dirname
from os.path import expanduser
from os.path import isabs
from os.path import isdir
from os.path import isfile
from os.path import join
from os.path import normpath

from ansible.module_utils.basic import AnsibleModule

# ---------------------------------------------------------------------

module      = None
image       = None
manifest    = None

# ---------------------------------------------------------------------

def close_files():
    global image

    if image is not None:
        image.close()

# ---------------------------------------------------------------------

def set_ownership(paths, owner=None, group=None):
    if (owner is None) and (group is None):
        return

    global module

    # Default values for leaving owner/group unchanged with chown():
    uid = gid = -1

    # Obtain the UID / GID for given owner/group:
    if owner:
        try:
            uid = pwd.getpwnam(owner).pw_uid
        except KeyError:
            module.fail_json(msg=f"owner '{owner}' not found in password database")

    if group:
        try:
            gid = grp.getgrnam(group).gr_gid
        except KeyError:
            module.fail_json(msg=f"group '{group}' not found in group database")

    try:
        for path in paths:
            chown(path, uid, gid, follow_symlinks=False)
    except PermissionError:
        module.fail_json(msg="failed to change permissions for path: "\
                              "%s [operation not permitted]" % abspath(path))
    except Exception as ex:
        module.fail_json(msg=str(ex))

# ---------------------------------------------------------------------

# Returns list of directory members (files/folders) that should be extracted,
# based on the src parameter. The returned list contains tarinfo objects, so
# it can be used by the tarfile.extractall() method.
def dir_members(tarfile, src, dest, globbed=False):
    global module

    members = []

    if globbed is True:
        subpath_len = len(src)
    else:
        subpath_len = len(dirname(normpath(src)))

    # Compensate for path '/' divider for any subfolders / files inside the folder:
    if '/' in src:
        subpath_len += 1

    for member in tarfile.getmembers():
        if (member.name.startswith(src) and
                (member.isdir() or member.isfile()) is True):
            # NOTE: We need to ensure that the destination path is a directory,
            #       when the source path corresponds to globbed path or folder,
            #       otherwise the extraction will fail.
            if dest is not None and (globbed or member.isdir()) is True:
                if isdir(dest) is False:
                    module.fail_json(msg=f"specified destination is not a directory: {dest}")

            if dest is None:
                member.path = member.path[subpath_len:]
            elif isdir(dest):
                member.path = join(dest, member.path[subpath_len:])
            else:
                member.path = dest

            members.append(member)

    # NOTE: If the globbing was used, then the first element of the members
    #       list will be an empty string, which is not a valid path. So we
    #       slice it off...
    if globbed is True:
        members = members[1:]

    return members

# ---------------------------------------------------------------------

# Throws KeyError exception when the specified src does not exist in container.
# Returns empty list when no extraction was done (i.e. file already exists).
# Returns list of paths for extracted files / folders otherwise.
def extract_path(src, dest=None, force=False):
    global module, image, manifest

    dest_file_exists = False

    # NOTE: To not make the code overly complicated, we are simply assuming
    #       that once the simplified globbing has been used, then we overwrite
    #       the destination files no matter what. I.e. globbing automatically
    #       assumes the 'force' flag to be set to 'true'...
    if src.endswith('/*'):
        src = src[:-2]              # To not confuse the tarfile.getmember() function.
        globbed = force = True
    else:
        globbed = False

        if dest is None:
            dest_file_exists = isfile(basename(src))
        elif isdir(dest):
            dest_file_exists = isfile(join(dest, basename(src)))
        else:
            dest_file_exists = isfile(dest)

    # NOTE: Directories will be always overwritten to make the code simpler.
    if not force and dest_file_exists:
        return []

    # NOTE: This part has been heavily inspired by this article:
    #       https://www.madebymikal.com/quick-hack-extracting-the-contents-of-a-docker-image-to-disk/
    #       As well as with my previous work with containers & Ansible...

    # We go through each layer of container image until we find the
    # requested path (file/folder). We start with the top layer first:
    for layer in reversed(manifest['Layers']):
        try:
            image_layer = tarfile.open(fileobj=image.extractfile(layer))

            image_layer.getmember(src)

            # NOTE: We have found the file inside the container image
            #       once we reach this point. Otherwise the KeyError
            #       exception is thrown (and the loop continues)...

            members_list = dir_members(image_layer, src, dest, globbed)

            # Extract the source path from container image into specified destination:
            image_layer.extractall(members=members_list)

            # Return just the extracted paths, not the list of tarfile objects:
            return [member.path for member in members_list]

        # Path to extract not found in this layer, let's continue...
        except KeyError:
            continue

        finally:
            image_layer.close()

    # Reaching this point means we were unable to find the requested
    # path (file/folder) inside the container image...
    raise KeyError(f"path not found in container image: {src}")

# ---------------------------------------------------------------------

def run_module():
    global module, image, manifest

    # Ansible Module initialization:
    module_args = dict(
        image = dict(type='path', required=True),
        src   = dict(type='path', required=False, default=None),
        dest  = dict(type='path', required=False, default=None),
        owner = dict(type='str',  required=False, default=None),
        group = dict(type='str',  required=False, default=None),
        paths = dict(type='list', required=False, default=None, elements='dict'),
        chdir = dict(type='path', required=False, default=None),
        force = dict(type='bool', required=False, default=False),
    )

    # Parsing of Ansible Module arguments:
    module = AnsibleModule(
        argument_spec=module_args,
        mutually_exclusive = [
            ('paths', 'src'),
            ('paths', 'dest'),
            ('paths', 'owner'),
            ('paths', 'group'),
        ],
        required_one_of = [
            ('paths', 'src'),
        ],
        supports_check_mode=False,
    )

    src   = module.params['src']
    dest  = module.params['dest']
    owner = module.params['owner']
    group = module.params['group']
    chdir = module.params['chdir']
    force = module.params['force']

    # We need to use the deepcopy here to not alter Ansible's invocation
    # arguments in the steps below...
    paths = copy.deepcopy(module.params['paths'])

    image_path = expanduser(module.params['image'])

    if chdir:
        chdir = expanduser(chdir)

    # NOTE: We use module.params[*] instead of the local variables, because
    #       we alter the local variables in the steps below, but we want to
    #       return the original values to Ansible in the result dictionary...
    result = dict(
        changed = False,
        image   = image_path,
        src     = module.params['src'],
        dest    = module.params['dest'],
        owner   = owner,
        group   = group,
        paths   = module.params['paths'],
        chdir   = chdir,
        force   = force,
    )

    # -----------------------------------------------------------------

    if chdir:
        try:
            os.chdir(chdir)
        except Exception as ex:
            module.fail_json(msg=str(ex), **result)

    # -----------------------------------------------------------------

    try:
        if image_path.endswith('tar.gz'):
            image = tarfile.open(image_path, 'r:gz')
        elif image_path.endswith('tar'):
            image = tarfile.open(image_path, 'r')
        else:
            module.fail_json(msg="only 'tar' and 'tar.gz' formats are supported", **result)
    except FileNotFoundError as ex:
        module.fail_json(msg="No such file or directory: %s" % image_path, **result)
    except Exception as ex:
        module.fail_json(msg=str(ex), **result)

    # Make sure we don't leave any file descriptors opened...
    atexit.register(close_files)

    # -----------------------------------------------------------------

    # We convert the single src / dest / owner / group options into a
    # paths list to simplify the follow up code...
    if src is not None:
        paths = [ {
            'src':  src,
            'dest': dest,
            'owner': owner,
            'group': group,
        } ]

    for path in paths:
        if 'src' not in path:
            module.fail_json(msg="'src' key must be defined when using 'paths' option", **result)
        elif isabs(path['src']) is True:
            # NOTE: Contents of container images apparently don't have
            #       the initial '/' in their paths, so we strip it...
            path['src'] = path['src'][1:]

        if 'dest' in path and path['dest'] is not None:
            path['dest'] = expanduser(path['dest'])

    # Check that we use full paths when 'chdir' was not set:
    if chdir is None:
        if isabs(image_path) is False:
            module.fail_json(msg="usage of relative paths requires 'chdir' option", **result)

        for path in paths:
            if 'dest' not in path:
                module.fail_json(msg="extracting paths to root level without explicit "
                                     "use of 'dest' option is not supported", **result)
            elif path['dest'] is not None and isabs(path['dest']) is False:
                module.fail_json(msg="usage of relative paths requires 'chdir' option", **result)

    # -----------------------------------------------------------------

    extracted_paths = []

    # json.loads() returns a list with single item, so we assign it directly:
    manifest = json.loads(image.extractfile('manifest.json').read())[0]

    for path in paths:
        src   = path.get('src')
        dest  = path.get('dest')
        owner = path.get('owner')
        group = path.get('group')

        try:
            extracted_list = extract_path(src, dest, force)
            extracted_paths.extend(extracted_list)

            set_ownership(extracted_list, owner, group)

        except Exception as ex:
            module.fail_json(msg=str(ex), **result)

    if extracted_paths:
        result['changed'] = True
        result['extracted'] = extracted_paths

    module.exit_json(**result)

# =====================================================================

def main():
    run_module()


if __name__ == '__main__':
    main()
