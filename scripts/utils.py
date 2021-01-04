from pathlib import Path


"""
    @returns the root path of the project.
"""
def get_project_root() -> Path:
    return Path(__file__).parent.parent.absolute()
