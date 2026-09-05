import os
from github import Github, GithubException

def push_to_github(
    local_file_path: str,
    repo_file_path: str,
    repo_name: str = "PauloRugani/tennis-data-analytics",
    branch: str = "main",
    token: str = None
) -> None:
    auth_token = token or os.getenv("GITHUB_TOKEN")
    if not auth_token:
        raise ValueError("GITHUB TOKEN NOT FOUND.")

    github = Github(auth_token)
    repo = github.get_repo(repo_name)

    with open(local_file_path, "rb") as f:
        content = f.read()

    commit_message = f"[PIPELINE] Updated {os.path.basename(repo_file_path)} [skip ci]"

    try:
        remote_file = repo.get_contents(repo_file_path, ref=branch)
        repo.update_file(
            path=repo_file_path,
            message=commit_message,
            content=content,
            sha=remote_file.sha,
            branch=branch
        )
        print(f"[PIPELINE] Updated: {repo_file_path}")
    except GithubException as e:
        if e.status == 404:
            repo.create_file(
                path=repo_file_path,
                message=f"[PIPELINE] Added {os.path.basename(repo_file_path)} [skip ci]",
                content=content,
                branch=branch
            )
            print(f"[PIPELINE] Created: {repo_file_path}")
        else:
            raise e