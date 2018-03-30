import os
import argparse

parser = argparse.ArgumentParser(description='Parse Input Path')
parser.add_argument('path', type=str, help='NECESSARY, root path based on which to list all the files')
args = parser.parse_args()

def isSkip(path, ignore_keys):
    for key in ignore_keys:
        if key in path:
            return True
    return False

root_dir = args.path
keys_ignore = ['.git', '.DS_Store', '.gitignore', 'maintenance-scripts', '.py', '.pyc', '.java', '.class', '.scala', 'code']

if not os.path.exists(root_dir):
    print('Invailaid working path {}'.format(root_dir))
    os.exit(1)

for root, directories, filenames in os.walk(root_dir):
    for filename in filenames:
        path_to_print = os.path.join(root, filename).replace(root_dir + os.sep, '')
        if isSkip(path_to_print, keys_ignore):
            continue
        print("- " + path_to_print)

