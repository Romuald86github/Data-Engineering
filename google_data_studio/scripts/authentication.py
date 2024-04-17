from google.oauth2 import service_account # type: ignore

def authenticate():
    SERVICE_ACCOUNT_FILE = 'path_to_service_account.json'
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/datastudio"]
    )
    return credentials
