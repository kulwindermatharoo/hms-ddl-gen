import os
from pathlib import Path


class CommonUtils:

    @staticmethod
    def get_project_root() -> str:
        # Get the current working directory
        current_directory = os.getcwd()

        # To get the project root directory, navigate up if necessary
        # Example: if your script is in a subfolder, you can go up to the root
        project_root = os.path.abspath(os.path.join(current_directory, os.pardir))

        print("Project Root:", project_root)

    @staticmethod
    def find_project_root(start_path: Path = Path.cwd()):
        for parent in start_path.parents:
            if (parent / 'setup.py').exists() or (parent / '.git').exists() \
                    or (parent / 'requirements.txt').exists():
                return parent
        return None
