from google.cloud import secretmanager


def get_secret(
    secret_path: str | None = None,
    project_number: str | None = None,
    secret_name: str | None = None,
) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = (
        f"projects/{project_number}/secrets/{secret_name}/versions/latest"
        if (secret_path is None) or (secret_path == "")
        else secret_path
    )
    response = client.access_secret_version(request={"name": name})

    return response.payload.data.decode("UTF-8")
