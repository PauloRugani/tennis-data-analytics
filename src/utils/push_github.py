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
        raise ValueError("Token do GitHub não fornecido nem encontrado na variável GITHUB_TOKEN.")

    g = Github(auth_token)
    repo = g.get_repo(repo_name)

    with open(local_file_path, "rb") as f:
        content = f.read()

    commit_message = f"chore(data): atualiza {os.path.basename(repo_file_path)} [skip ci]"

    try:
        remote_file = repo.get_contents(repo_file_path, ref=branch)
        repo.update_file(
            path=repo_file_path,
            message=commit_message,
            content=content,
            sha=remote_file.sha,
            branch=branch
        )
        print(f"[GitHub] Atualizado: {repo_file_path}")
    except GithubException as e:
        if e.status == 404:
            repo.create_file(
                path=repo_file_path,
                message=f"chore(data): adiciona {os.path.basename(repo_file_path)} [skip ci]",
                content=content,
                branch=branch
            )
            print(f"[GitHub] Criado: {repo_file_path}")
        else:
            raise e