
from .errors import *
from .connection import Connection


try:
    from google.appengine.api import app_identity
    ON_GAE = True
except ImportError:
    ON_GAE = False


def connect(project_id, instance_id, database_id, credentials_json=None):
    if not credentials_json:
        if ON_GAE:
            auth_token, _ = app_identity.get_access_token(
                'https://www.googleapis.com/auth/cloud-platform'
            )
        else:
            raise RuntimeError("You must specify the path to the credentials file")
    else:
        from oauth2client.client import GoogleCredentials
        credentials = GoogleCredentials.from_stream(credentials_json)
        credentials = credentials.create_scoped('https://www.googleapis.com/auth/cloud-platform')
        access_token_info = credentials.get_access_token()
        auth_token = access_token_info.access_token

    return Connection(
        project_id, instance_id, database_id, auth_token
    )
