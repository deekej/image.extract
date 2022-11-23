#!/usr/bin/python

# Copyright: (c) 2022, Dee'Kej <devel@deekej.io>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: image.extract

short_description: Extracts the contents of given container image

description:
    - This module extract the specified path from the given container image - without the need to have M(docker) or M(podman) installed...

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
          - Path to the file / folder inside the container that needs to be extracted.
        required: true
        default: null
        type: path

    dest:
        description:
            - Path where the extracted file / folder should be placed.
            - If no full path is used, then specifies the requested name of extracted file / folder.
            - If omitted, then M(basename) of M(src) option is used as the extracted file / folder name.
            - Full path can't be used together with M(chdir) option.
        required: false
        default: null
        type: path

    chdir:
        description:
            - Directory to change into where the extracted file / folder will be placed.
            - By default this is current working directory.
        requied: false
        default: null
        type: path

    owner:
        description:
            - Name of the owner for all the extracted files.
            - Set by UNIX's M(chown) utility.
        required: false
        default: null
        type: string

    group:
        description:
            - Name of the group for all the extracted files.
            - Set by UNIX's M(chown) utility.
        required: false
        default: null
        type: string

    force:
        description:
            - Runs the container image extraction even if previously extracted file / folder exists.
        required: false
        default: false
        type: boolean

author:
    - Dee'Kej (@deekej)
'''

EXAMPLES = r'''
- name: Extract file from container image
  image.extract:
    image:  rhel-9-ubi.tar
    src:    /usr/lib/os-release
    owner:  ansible-automation
    group:  ansible-automation

- name: Extract file from container image to a different location
  image.extract:
    image:  rhel-9-ubi.tar
    src:    /usr/lib/os-release
    chdir:  /tmp

- name: Extract file from container image and rename it
  image.extract:
    image:  rhel-9-ubi.tar
    src:    /usr/lib/os-release
    dest:   rhel-9-release
    chdir:  ~/ansible

- name: Extract file from container image to a specified full path
  image.extract:
    image:  rhel-9-ubi.tar
    src:    /usr/lib/os-release
    dest:   /home/deekej/ansible/rhel-9-release

- name: Extract folder from container image
  image.extract:
    image:  rhel-9-ubi.tar
    src:    /etc
'''

# =====================================================================

import grp
import hashlib
import json
import os
import pwd
import sys
import tarfile

from ansible.module_utils.basic import AnsibleModule

def run_module():
    # Ansible Module initialization:
    module_args = dict(
        image = dict(type='raw',  required=True),
        src   = dict(type='path', required=True),
        dest  = dict(type='path', required=False, default=None),
        chdir = dict(type='path', required=False, default=None),
        owner = dict(type='str',  required=False, default=None),
        group = dict(type='str',  required=False, default=None),
        force = dict(type='bool', required=False, default=False)
    )

    # Parsing of Ansible Module arguments:
    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    # NOTE: Contents of container images apparently don't have initial
    #       the initial '/' in their paths, so we strip it...
    src   = module.params['src'][1:]
    dest  = None
    chdir = None
    owner = module.params['owner']
    group = module.params['group']
    force = module.params['force']
    dest_exists = False

    image_path = os.path.expanduser(module.params['image'])

    if module.params['dest']:
        dest  = os.path.expanduser(module.params['dest'])

    if module.params['chdir']:
        chdir = os.path.expanduser(module.params['chdir'])

    result = dict(
        changed=False,
        image = image_path,
        src = module.params['src'],
        dest = dest,
        chdir = chdir,
        owner = owner,
        group = group,
        force = force
    )

    # Check for parameters collision:
    if chdir and dest and os.path.isabs(dest):
        module.fail_json(msg="full path for 'dest' is mutually exclusive "\
                             "with 'chdir' parameter", **result)

    # -----------------------------------------------------------------

    # Prepare the destination filename if it wasn't specified:
    if not dest:
        dest = os.path.basename(src)
        result['dest'] = dest

    # By default we extract the file / folder into the same folder as
    # where the container image is located -- unless the 'chdir'
    # parameter was used, or the 'dest' parameter is an absolute path...
    if chdir:
        result_dir = chdir
    elif os.path.isabs(dest):
        result_dir = os.path.dirname(dest)
    else:
        result_dir = os.path.dirname(image_path)

    os.chdir(result_dir)

    # Check if the destination file already exists:
    if os.path.isfile(dest):
        dest_exists = True

    # -----------------------------------------------------------------
    # NOTE: This part has been heavily inspired by this article:
    #       https://www.madebymikal.com/quick-hack-extracting-the-contents-of-a-docker-image-to-disk/
    #       As well as with my previous work with containers & Ansible.

    if image_path.endswith('tar.gz'):
        image = tarfile.open(image_path, 'r:gz')
    elif image_path.endswith('tar'):
        image = tarfile.open(image_path, 'r')
    else:
        module.fail_json(msg="only 'tar' and 'tar.gz' formats are supported", **result)

    manifest = json.loads(image.extractfile('manifest.json').read())[0]

    # We go through each layer of container image until we find the
    # requested path (file/folder). We start with the top layer first.
    for layer in reversed(manifest['Layers']):
        image_layer = tarfile.open(fileobj=image.extractfile(layer))

        try:
            tarinfo = image_layer.getmember(src)

            # NOTE: We have found the file inside the container image
            #       once we reach this point. Otherwise the KeyError
            #       exception is thrown...
            if not (tarinfo.isfile() or tarinfo.isdir()):
                module.fail_json(msg="only files or folders can be extracted", **result)

            if module.check_mode:
                if not dest_exists or force:
                    result['changed'] = True
                    module.exit_json(**result)

            if not force and dest_exists and tarinfo.isfile():
                # Get the SHA-1 checksum of file that needs extracting:
                tflo_hash = hashlib.sha1()
                tflo = image_layer.extractfile(tarinfo)

                data = tflo.read()

                while data:
                    tflo_hash.update(data)
                    data = tflo.read()

                # Get the SHA-1 checksum of the existing destination file:
                with open(dest, 'rb') as dest_file:
                    dest_file_hash = hashlib.sha1()

                    data = dest_file.read()

                    while data:
                        dest_file_hash.update(data)
                        data = dest_file.read()

                # Nothing to do? Bail out...
                if tflo_hash.hexdigest() == dest_file_hash.hexdigest():
                    module.exit_json(**result)

            # Extract the file from container image into current directory:
            image_layer.extract(tarinfo)

            # Rename the file to the requested name:
            os.rename(os.path.basename(src), dest)

            # ---------------------------------------------------------

            # Default values for leaving owner/group unchanged with chown():
            uid = -1
            gid = -1

            # Obtain the UID / GID for given owner/group:
            if owner:
                try:
                    uid = pwd.getpwnam(owner).pw_uid
                except KeyError:
                    module.fail_json(msg="owner '%s' not found in password database" % owner)

            if group:
                try:
                    gid = grp.getgrnam(group).gr_gid
                except KeyError:
                    module.fail_json(msg="group '%s' not found in group database" % group)

            if owner or group:
                try:
                    if os.path.isdir(dest):
                        # NOTE: The 'dirnames' are ignored on purpose. More info:
                        # https://stackoverflow.com/a/57458550/3481531
                        for dirpath, dirnames, filenames in os.walk(dest):
                            os.chown(dirpath, uid, gid, follow_symlinks=False)

                            for filename in filenames:
                                os.chown(os.path.join(dirpath, filename), uid, gid, follow_symlinks=False)
                    else:
                        os.chown(dest, uid, gid, follow_symlinks=False)
                except PermissionError:
                    module.fail_json(msg="failed to change permissions for path: "\
                                         "%s [operation not permitted]" % os.path.abspath(dest))

            result['changed'] = True
            module.exit_json(**result)

        except KeyError:
            # Path to extract not found in this layer, let's continue...
            continue

    # -----------------------------------------------------------------

    # Reaching this point means we were unable to find the requested
    # path (file/folder) inside the container image...
    module.fail_json(msg="file not found in container image: %s" % src)

# =====================================================================

def main():
    run_module()


if __name__ == '__main__':
    main()
