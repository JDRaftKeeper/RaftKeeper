import os
import sys
import json

from github.Auth import AppAuth
from github import Github


app_id = "995720"
installation_id = 54780428

private_key_part_1 = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAyN+CJniZ5AXyAdcHmfwq0ltaaSLBahjsndlJxJY4zxS3ywxC
"""

private_key_part_2 = """ornAzDEKp4VtBf9EWIraMsCk1Ak9Wp0yICNngJ+Ch13173xk2nkRNBRYAnq+Ln+x
3VetS7hNMkHrSRHZezapzMCki9C0E7a9ks2F3sMwj8rKKhCvspMWGDBTMan5pKkc
YLlOhA27MF66SOIdDAqbi9m0pKeoNvaFaD5hRykYE+bDjnfTXSRWzPragEk5pbKl
sROzSDHqNYDH1d3QK4D1Uk7uT5ZQnkQuWFfliLbSaqPk4NhRwQx4jCqbkQZ4HvPm
8q8I5fql1gDwOxPr1eTLOU00C5x3wcwnFT+MXwIDAQABAoIBAC38FNRvyXMM9WD8
c+4Jb1gmt6TX4wVB3XEpXBzX8vtdF9Iw5VRRR9S26WR+Q/4aeO/4IYl61oD/+H60
+9Olpz0nxv7sQK/pf0EQdCLDAX7X0I/ehb5RIwfxkiKOOqnIn0v4sJiCBWlIhuD4
dZ+U0y+y6XwRhYRpu38a8vToozL7+WX/Wu656FK0H0huH53EazrxxD6JLpoIFErK
ojU5vzPJUgTdWnMZAdlEOjcO0Qg/XwDlzmQFga5duIzkt9CcmlOwRoWsVBph6YP8
Qzz2sjHslzoLo0Nmc+HVZLE+uyKjeI5UFfPRu5c+rZgbwR27fDJY/O/JsFbV8wZm
5u504TkCgYEA849asWpIiWWg4U00txtM1etNe450x9bo6LgQ2QtcJTk5OPGuxUt3
sX51upYHat8VbEuEiEe70QEUySwqOiK6ofLn8zr8tPmg0smy0zt1DJy/tPHQ4ubX
A9DKMqYxHJPS4iRi5R7sOIjNFBfA2iUPEesQRPXAIMkIyevv6LBfg9UCgYEA0yIB
bftBTJc1WDbaOU339GYm8rLjiDHyK9DAOC548m85Bm1b45EtMoB5e+0VY/C10eLy
A2A7t7CpA9kLZRVlx4voQ21bTjEnO470GN6SfmhcLZ65WzvbZvLHj+zGjkV8+Lot
Av02h6hnwfwQhvN9XsZPyNk4IdcXK7045DnGzWMCgYAcF5HHWtHo/w7STbxhzkVL
eythr+mqTxBoHyraTeQf6vy9o6qb2PuCPmrHzZwnaHmpFwC/Uz7HeY9zMKPiNrU+
Dq1QMaKKISy6g0cb9ASpIr892JJWSXfNWdyogOCzQh2VtcquUKXAU48L3T2CK7oU
P/+NZKb3YRihaZQvS4CIzQKBgQC6UsxILu/Vk6u0CdRTtgcYW/4LOOurichZ+oNo
ETsTWCxPC7uH/NqSMucDAptZ81fBvjIt4INS/Ehr6OMxdcy4aTO0LZHiU2Z4HRQ1
zlYh0B9o8yZI6W4aUC7lSOOBMrmzFzoZ5TR2S5wliTlcnw0I0qIecfQjiRods4O9
hW94WQKBgHMxir/eMLR8z9hj1jpJh+eN3Uo6UM+5dzgqNUn4CJC2Tt6tIZPoxdhX
MimMH9yrRcCe068VlvqN+AsTTpkpceJT52dV5iXuEm1DoYgvhvD19MMlW/QmOY8w
5GHXzNf9wt2HkMZZcpCasMTfWW3LvtEomcA3USQbZt5NgX7QjAVo
-----END RSA PRIVATE KEY-----"""

private_key = private_key_part_1 + private_key_part_2

def comment_on_pr(content, title):
    print("content:", content)
    if content is None:
        raise Exception("Content is required")

    app_auth = AppAuth(app_id, private_key)
    g = Github(auth=app_auth.get_installation_auth(installation_id))

    # Get the pull request number and repository details
    repo_name = os.environ['GITHUB_REPOSITORY']
    print("repo_name:", repo_name)

    # Read the event payload
    with open(os.environ['GITHUB_EVENT_PATH'], 'r') as f:
        event_payload = json.load(f)

    # Extract the pull request number
    pull_request_number = event_payload['pull_request']['number']
    print("pull_request_number:", pull_request_number)

    repo = g.get_repo(repo_name)
    pull_request = repo.get_pull(int(pull_request_number))

    # Get all comments on the pull request
    comments = pull_request.get_issue_comments()

    # Check if a comment by raftkeepeer-robot[bot] exists
    comment_found = False
    for comment in comments:
        print("find comment:", comment)
        if comment.user.login == "raftkeepeer-robot[bot]" and title in comment.body:
            # Update the existing comment
            comment.edit(content)
            comment_found = True
            break

    # If not, create a new comment
    if not comment_found:
        pull_request.create_issue_comment(content)


def get_artifact_url(artifact_name):
    # Authenticate with GitHub
    app_auth = AppAuth(app_id, private_key)
    g = Github(auth=app_auth.get_installation_auth(installation_id))

    # Get the repository
    repo_name = os.environ['GITHUB_REPOSITORY']
    repo = g.get_repo(repo_name)

    # Get the specific workflow run
    run_id = os.environ['GITHUB_RUN_ID']
    print("run_id:", run_id)
    workflow_run = repo.get_workflow_run(int(run_id))

    # List artifacts for the workflow run
    artifacts = workflow_run.get_artifacts()

    # Assuming you want the first artifact, get its download URL
    for artifact in artifacts:
        if artifact.name == artifact_name:
            return artifact.archive_download_url

    return None
