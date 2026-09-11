import base64
import os
from github import Github, GithubException

class GithubHandler:
    def __init__(self, token: str = None, default_repo: str = "PauloRugani/tennis-data-analytics", default_branch: str = "main"):
        self.token = token or os.getenv("GITHUB_TOKEN")
        if not self.token:
            print("[GitHub] GITHUB_TOKEN not found")
            
        self.default_repo = default_repo
        self.default_branch = default_branch
        self.github = Github(self.token) if self.token else None

    def fetch_file(self, repo_file_path: str, local_dest_path: str, repo_name: str = None) -> bool:
        if not self.github:
            return False
            
        repo_name = repo_name or self.default_repo
        try:
            repo = self.github.get_repo(repo_name)
            file_content = repo.get_contents(repo_file_path, ref=self.default_branch)

            if file_content.encoding == "base64" and file_content.content:
                raw_bytes = base64.b64decode(file_content.content)
            else:
                blob = repo.get_git_blob(file_content.sha)
                raw_bytes = base64.b64decode(blob.content)

            if len(raw_bytes) == 0 and os.path.exists(local_dest_path) and os.path.getsize(local_dest_path) > 0:
                return False

            os.makedirs(os.path.dirname(local_dest_path), exist_ok=True)
            with open(local_dest_path, "wb") as f:
                f.write(raw_bytes)

            return True
        except Exception as e:
            print(f"[GitHub] {e}")
            return False

    def push_file(self, local_file_path: str, repo_file_path: str, repo_name: str = None, branch: str = None) -> None:
        if not self.github:
            raise ValueError("GITHUB TOKEN NOT FOUND.")
            
        repo_name = repo_name or self.default_repo
        branch = branch or self.default_branch
        repo = self.github.get_repo(repo_name)

        with open(local_file_path, "rb") as f:
            content = f.read()

        commit_message = f"[PIPELINE] Updated {os.path.basename(repo_file_path)}"

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
                    message=f"[PIPELINE] Added {os.path.basename(repo_file_path)}",
                    content=content,
                    branch=branch
                )
                print(f"[PIPELINE] Created: {repo_file_path}")
            else:
                raise e
