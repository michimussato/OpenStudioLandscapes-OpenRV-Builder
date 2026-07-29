import textwrap
from typing import (
    List,
    Dict,
)

import requests
import json

from dagster import (
    run_status_sensor,
    sensor,
    SensorEvaluationContext,
    DagsterRunStatus,
    RunStatusSensorContext,
    DefaultSensorStatus,
    RunRequest,
    AssetKey,
    SkipReason,
    RunConfig,
)

from OpenStudioLandscapes.OpenRV_Builder import ASSET_HEADER
from OpenStudioLandscapes.OpenRV_Builder.config.enums import (
    CY,
    OS,
)
from OpenStudioLandscapes.OpenRV_Builder.resources import (
    VFXReferencePlatformResource,
    NtfyResource,
    AutoBuilderResource,
)
from OpenStudioLandscapes.OpenRV_Builder.jobs import materialize_new_commit_job

from OpenStudioLandscapes.engine.utils import get_base64_auth_str


# Todo
#  - [ ] get address for dagster webserver
#        - [ ] Is this even possible?
#        - [ ] does it even make sense?
#              not really. the code-location and web-server modules are
#              totally separate. therefore, we need to specify the web-server
#              manually.
#        - `python3.11 -m dagster_webserver --help`
#        - `ps -aux | grep dagster_webserver`
#        - https://github.com/dagster-io/dagster/blob/becd5d76788f21a52933e1384e146eac2d443dd8/python_modules/dagster-webserver/dagster_webserver/cli.py#L284
#        - /home/michael/git/repos/OpenStudioLandscapes-OpenRV-Builder/.venv/lib/python3.11/site-packages/dagster/_grpc/server.py
#        - /home/michael/git/repos/OpenStudioLandscapes-OpenRV-Builder/.venv/lib/python3.11/site-packages/dagster_webserver/cli.py
#  - [ ] convert cursor from List[str] to List[Dict[str]] so that we can store more info than
#        just the commit string


"""
- [Publishing](https://ntfy.pangolin.meemoo.ch/docs/publish/)
- [Publish as JSON](https://ntfy.pangolin.meemoo.ch/docs/publish/#publish-as-json)
- [Message Priority](https://ntfy.pangolin.meemoo.ch/docs/publish/#message-priority)
- [Emoji reference](https://ntfy.pangolin.meemoo.ch/docs/emojis/)
"""


# https://docs.dagster.io/guides/automate/sensors/run-status-sensors
@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="openrv_builder_success_status_sensor",
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=15,
    description="Sensor to detect successful runs.",
)
def ntfy_on_run_success(
    context: RunStatusSensorContext,
    ntfy_resource: NtfyResource,
):

    context.log.info(f"{ntfy_resource.ntfy_enable = }")

    if ntfy_resource.ntfy_enable:

        auth = {}
        if ntfy_resource.require_auth:
            auth = {
                "Authorization": get_base64_auth_str(
                    username=ntfy_resource.ntfy_username,
                    password=ntfy_resource.ntfy_password,
                    prefix="Basic",
                ),
            }

        run_url = f"{ntfy_resource.dagster_webserver_protocol}://{ntfy_resource.dagster_webserver_host}:{ntfy_resource.dagster_webserver_port}/runs/{context.dagster_run.run_id}"

        # To generate the Authorization header, use standard base64 to encode the colon-separated <username>:<password> and prepend the word Basic, i.e. Authorization: Basic base64(<username>:<password>).
        # Will Pangolin be able to deal with that?
        # -> "Basic Header Auth" for ntfy
        payload = json.dumps(
            {
                "topic": ntfy_resource.ntfy_topic,
                "message": f"Dagster Run [{context.dagster_run.run_id}]({run_url}) finished successfully.",
                "title": "Dagster Run Succeeded",
                "tags": [
                    "hammer_and_pick",
                    "white_check_mark",
                ],
                "priority": 3,
                "actions": [
                    {
                        "action": "view",
                        "label": "Dagster Run",
                        "url": run_url,
                    },
                ]
            }
        )

        headers = {
            "Content-Type": "application/json",
            "Markdown": "yes",
            **auth,
        }

        result = requests.request(
            method="POST",
            url=ntfy_resource.ntfy_url,
            headers=headers,
            data=payload,
        )

        context.log.info(result)


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    name="openrv_builder_failure_status_sensor",
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=15,
    description="Sensor to detect failed runs.",
)
def ntfy_on_run_failure(
    context: RunStatusSensorContext,
    ntfy_resource: NtfyResource,
):

    context.log.info(f"{ntfy_resource.ntfy_enable = }")

    if ntfy_resource.ntfy_enable:

        auth = {}
        if ntfy_resource.require_auth:
            auth = {
                "Authorization": get_base64_auth_str(
                    username=ntfy_resource.ntfy_username,
                    password=ntfy_resource.ntfy_password,
                    prefix="Basic",
                ),
            }

        run_url = f"{ntfy_resource.dagster_webserver_protocol}://{ntfy_resource.dagster_webserver_host}:{ntfy_resource.dagster_webserver_port}/runs/{context.dagster_run.run_id}"

        payload = json.dumps(
            {
                "topic": ntfy_resource.ntfy_topic,
                "message": f"Dagster Run [{context.dagster_run.run_id}]({run_url}) failed.",
                "title": "Dagster Run Failed",
                "tags": [
                    "hammer_and_pick",
                    "warning",
                ],
                "priority": 4,
                "actions": [
                    {
                        "action": "view",
                        "label": "Dagster Run",
                        "url": run_url,
                    },
                ]
            }
        )

        headers = {
            "Content-Type": "application/json",
            "Markdown": "yes",
            **auth,
        }

        result = requests.request(
            method="POST",
            url=ntfy_resource.ntfy_url,
            headers=headers,
            data=payload,
        )

        context.log.info(result)


@sensor(
    job=materialize_new_commit_job,
    # We have max. 60 requests per hour.
    # https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2026-03-10#checking-the-status-of-your-rate-limit
    # A run takes a faily long time, so we want to keep the intervals
    # higher than a build time. I guess once or twice a day would be
    # fairly reasonable.
    minimum_interval_seconds=3600 * 24,
    name="openrv_builder_commits_sensor",
    default_status=DefaultSensorStatus.STOPPED,
    description=textwrap.dedent(
        """\
        This sensor checks for now commits on the AcademySoftwareFoundation/OpenRV
        repository. If a new commit was found, the sensor will start a new OpenRV
        build pipeline starting with the `rv_clone_write_dockerfile` downstream.
        """
    )
)
def check_for_new_commits(
        context: SensorEvaluationContext,
        ntfy_resource: NtfyResource,
        auto_builder_resource: AutoBuilderResource,
):

    context.log.debug(f"{context.cursor = }")

    cursor = json.loads(context.cursor) if context.cursor else [auto_builder_resource.consider_commits_starting_from]

    previous_cursor = cursor
    context.log.info(f"{previous_cursor = }")

    response_github: requests.Response = requests.get(
        url="https://api.github.com/repos/AcademySoftwareFoundation/OpenRV/commits?sha=main",
    )

    headers = response_github.headers
    rate_limit = int(headers["X-RateLimit-Limit"])
    rate_limit_used = int(headers["X-RateLimit-Used"])
    rate_limit_remaining = int(headers["X-RateLimit-Remaining"])

    context.log.info(f"{rate_limit           = }")
    context.log.info(f"{rate_limit_used      = }")
    context.log.info(f"{rate_limit_remaining = }")

    context.log.info(f"{response_github.status_code = }")
    context.log.debug(f"JSON response_github: {json.dumps(response_github.json(), indent=2, default=str)}")

    if bool(rate_limit_remaining):

        # newest first in list
        commits: List[Dict] = response_github.json()
        # commits[0]: {'sha': '71e3bf2d5f6ccca1bf8f6f7112f608702a31a769', 'node_id': 'C_kwDOIV0UedoAKDcxZTNiZjJkNWY2Y2NjYTFiZjhmNmY3MTEyZjYwODcwMmEzMWE3Njk', 'commit': {'author': {'name': 'Roger Nelson', 'email': 'roger.nelson@autodesk.com', 'date': '2026-07-07T22:17:28Z'}, 'committer': {'name': 'GitHub', 'email': 'noreply@github.com', 'date': '2026-07-07T22:17:28Z'}, 'message': 'fix: SG-43961 text size in otio_reader (#1337)\n\n### Fix test sizing in OTIO reader\n\n### Summarize your change.\n\nAfter https://github.com/AcademySoftwareFoundation/OpenRV/pull/1334 I\nneed to make the same changes as were made there to the text scaling.\nText sizing is now simply a normalized value, so just input it directly\nfrom the OTIO Annotations as such.\n\n### Describe the reason for the change.\n\nFix image scaling in OTIO import.\n\n### Describe what you have tested and on which operating system.\n\nMac OS 26.5.1\n\nSigned-off-by: Roger Nelson <roger.nelson@autodesk.com>', 'tree': {'sha': '43e25ae0ed1dffa8fbfc11d858ad209966afc119', 'url': 'https://api.github.com/repos/AcademySoftwareFoundation/OpenRV/git/trees/43e25ae0ed1dffa8fbfc11d858ad209966afc119'}, 'url': 'https://api.github.com/repos/AcademySoftwareFoundation/OpenRV/git/commits/71e3bf2d5f6ccca1bf8f6f7112f608702a31a769', 'comment_count': 0, 'verification': {'verified': True, 'reason': 'valid', 'signature': '-----BEGIN PGP SIGNATURE-----\n\nwsFcBAABCAAQBQJqTXr4CRC1aQ7uu5UhlAAA1tIQAEG3NG3KR+bYezl6PfAz7CvX\nJa2LMlXrtRazqrxC0/+i1mP6S+Sv0lmebShWORlhWnEnq1GnQbuFBBqKNqj+zeva\ngBbILQAgEkP+TrRV2UdvoRi4VT9jxI1GdjYie7OgnF0T0yMrlA/zizVm6BaDftLy\n4Ae5aEITJ7gadcApY9cP4JfV31Mazp+GgLDi8ktaEVrUo3OXMTYxuROI7o/cwEuB\nqooxKjo6Wa4EtAwkV/1BUDCi2p8vwF35dvNONeBJEiS8crKN6KBQxCYuGIhRt9zd\nHgjIooz0cXOXCQm/3ucIHBGxEbt0S/FWxg4H4IqmwzQfcIjWzvzn6g3bD/6BIeLp\nw3Jtfv5PAWWZMlLFEAMz6PvcAxlz4V24n3RlY+YKwx1Q9U17eOVaFNceeKPgtr2d\nbTSDzISqt3yUEj8oYjZy0LR48kAWVPAeTsShmxDFOpANI/GbJFoLIlLH9QqGKZC5\nm+hwL9JuRD6x8jo7bgiFV7DJpjIwF8uSNT92HacRsQwOokjklmqlJ7CmE0dvCr92\nOuYvNigLFXWNL/vn/lZWZDfe3Fc5/ySMfi8/BLDwAu4MDXdfl5eP6ENW0zwSavG9\niD9+WL4PhZvJWFmbuZOJp+FxpGrITGyX95zDa44YX5oGwI/fIxzSROfX20dkNX4F\nPRO+EifzoX7Lphi3qkJ2\n=/AX1\n-----END PGP SIGNATURE-----\n', 'payload': 'tree 43e25ae0ed1dffa8fbfc11d858ad209966afc119\nparent b0c430b0695d1fab29c7d8225fdbecf9d5150e6a\nauthor Roger Nelson <roger.nelson@autodesk.com> 1783462648 -0400\ncommitter GitHub <noreply@github.com> 1783462648 +0000\n\nfix: SG-43961 text size in otio_reader (#1337)\n\n### Fix test sizing in OTIO reader\n\n### Summarize your change.\n\nAfter https://github.com/AcademySoftwareFoundation/OpenRV/pull/1334 I\nneed to make the same changes as were made there to the text scaling.\nText sizing is now simply a normalized value, so just input it directly\nfrom the OTIO Annotations as such.\n\n### Describe the reason for the change.\n\nFix image scaling in OTIO import.\n\n### Describe what you have tested and on which operating system.\n\nMac OS 26.5.1\n\nSigned-off-by: Roger Nelson <roger.nelson@autodesk.com>', 'verified_at': '2026-07-07T22:17:28Z'}}, 'url': 'https://api.github.com/repos/AcademySoftwareFoundation/OpenRV/commits/71e3bf2d5f6ccca1bf8f6f7112f608702a31a769', 'html_url': 'https://github.com/AcademySoftwareFoundation/OpenRV/commit/71e3bf2d5f6ccca1bf8f6f7112f608702a31a769', 'comments_url': 'https://api.github.com/repos/AcademySoftwareFoundation/OpenRV/commits/71e3bf2d5f6ccca1bf8f6f7112f608702a31a769/comments', 'author': {'login': 'rogernelson', 'id': 8494931, 'node_id': 'MDQ6VXNlcjg0OTQ5MzE=', 'avatar_url': 'https://avatars.githubusercontent.com/u/8494931?v=4', 'gravatar_id': '', 'url': 'https://api.github.com/users/rogernelson', 'html_url': 'https://github.com/rogernelson', 'followers_url': 'https://api.github.com/users/rogernelson/followers', 'following_url': 'https://api.github.com/users/rogernelson/following{/other_user}', 'gists_url': 'https://api.github.com/users/rogernelson/gists{/gist_id}', 'starred_url': 'https://api.github.com/users/rogernelson/starred{/owner}{/repo}', 'subscriptions_url': 'https://api.github.com/users/rogernelson/subscriptions', 'organizations_url': 'https://api.github.com/users/rogernelson/orgs', 'repos_url': 'https://api.github.com/users/rogernelson/repos', 'events_url': 'https://api.github.com/users/rogernelson/events{/privacy}', 'received_events_url': 'https://api.github.com/users/rogernelson/received_events', 'type': 'User', 'user_view_type': 'public', 'site_admin': False}, 'committer': {'login': 'web-flow', 'id': 19864447, 'node_id': 'MDQ6VXNlcjE5ODY0NDQ3', 'avatar_url': 'https://avatars.githubusercontent.com/u/19864447?v=4', 'gravatar_id': '', 'url': 'https://api.github.com/users/web-flow', 'html_url': 'https://github.com/web-flow', 'followers_url': 'https://api.github.com/users/web-flow/followers', 'following_url': 'https://api.github.com/users/web-flow/following{/other_user}', 'gists_url': 'https://api.github.com/users/web-flow/gists{/gist_id}', 'starred_url': 'https://api.github.com/users/web-flow/starred{/owner}{/repo}', 'subscriptions_url': 'https://api.github.com/users/web-flow/subscriptions', 'organizations_url': 'https://api.github.com/users/web-flow/orgs', 'repos_url': 'https://api.github.com/users/web-flow/repos', 'events_url': 'https://api.github.com/users/web-flow/events{/privacy}', 'received_events_url': 'https://api.github.com/users/web-flow/received_events', 'type': 'User', 'user_view_type': 'public', 'site_admin': False}, 'parents': [{'sha': 'b0c430b0695d1fab29c7d8225fdbecf9d5150e6a', 'url': 'https://api.github.com/repos/AcademySoftwareFoundation/OpenRV/commits/b0c430b0695d1fab29c7d8225fdbecf9d5150e6a', 'html_url': 'https://github.com/AcademySoftwareFoundation/OpenRV/commit/b0c430b0695d1fab29c7d8225fdbecf9d5150e6a'}]}
        commits_sha: List[str] = [sha["sha"] for sha in commits]
        # commits_sha = ['07cd4c26085df4e1250e9cee7bfd69b97dfe8175', '584084fbdde0b8717f2fe82e5abcd18e627ca300',
        #                'd29a83bb389e4838234e01006e5e8c300127733d', '71e3bf2d5f6ccca1bf8f6f7112f608702a31a769',
        #                'b0c430b0695d1fab29c7d8225fdbecf9d5150e6a', 'af258f90779a274fa050b1481cd3cd7d4c037a31',
        #                'f702cd926b58a86da4616e29980c8ef8c378aba4', '9831b1ac171459ef2af9a1040dab88ac0d4618bc',
        #                '3dad8d05a3cd8b02c6ef86983cc9291cc01fbd8b', '0c922732db5aa7ae59ff7f00e12e12f161811c79',
        #                'd43f3c341d4330ac745d5e9569ab93a9787cb91d', '4517984b09a0760e5bf200b3004f09252253e200',
        #                '47144967c4dcaccfac281f19fe916cbca58f2850', '1f435dffe29e44597da5a43399c989b5e3a23598',
        #                'e8147ef66bf6b26dab7c7a9a7d8491f4d0f3ffc8', 'a5f07e711064f48b0e4a8e5a1c79dbec7b97d70d',
        #                'f0257e63820f7a3906c357289422fea2ea8c8a5b', '7f598d6a02c3d71e483ab3fb658dddc2de738359',
        #                '22650944200e0ec00e41be5f8229f8a878c7af67', '50554231d46d93bd0ea662b19c6f1b36b2f79dc5',
        #                '195a654769ed97d30ac55cbe968866f82e5f245c', 'a1cf3926aed3069e47825f60e4bc8fb3c7df4107',
        #                '28536c86f9e7397840c01f1986481a83b60f27df', '0e7555b701ff2688703bf572d0fd89842b0ca11f',
        #                'a1edaa3fb3f4e5a8b259c9912cb9b6ce798566e8', '0686cb5e0977447718c96033e1bd25bd946c1b04',
        #                'b2e4c9dd59fa2237c5d02749117c8c5241a81cb0', '0bd39a844ada7d8b3961c1bf477c32c88dee7d6f',
        #                '3e22a8f51004ec696e3268381a6e5218498f8b96', '6219ece6f3efc1e8f86fc6b2521c39e3fba6b3d8']
        context.log.debug(f"{commits_sha = }")

        # oldest sha in cursor
        cursor_oldest = cursor[-1]
        index_oldest_commit_in_cursor = commits_sha.index(cursor_oldest)

        # drop all older than last in cursor but include oldest in cursor
        commits_sha_keep = commits_sha[:index_oldest_commit_in_cursor+1]
        context.log.debug(f"{commits_sha_keep = }")

        # drop all intersecting
        sha_unbuilt = [item for item in commits_sha_keep if item not in cursor]
        context.log.debug(f"{sha_unbuilt = }")

        context.log.debug(f"{auto_builder_resource.build_newest_only = }")
        if auto_builder_resource.build_newest_only:
            sha_unbuilt = sha_unbuilt[:1]

        context.log.debug(f"{sha_unbuilt = }")

        # process oldest first
        for commit_sha in reversed(sha_unbuilt):

            run_key = f"{commit_sha}"
            # nested loops:
            # need to loop over CY and OS eventually:
            # - https://www.iditect.com/faq/python/how-to-iterate-over-columns-of-a-matrix-in-python.html
            cy: str
            # cy is str here because we cannot json serialize the values as enum members (yet):
            # - config_AutoBuilderResource.write_text(
            #        data=AutoBuilderResource().model_dump_json(
            #            indent=2,
            #            fallback=str,
            #        )
            #    )
            #   [...]
            #   The above exception was caused by the following exception:
            #   dagster._serdes.errors.SerializationError: Can only serialize whitelisted Enums, received CY.
            #   Descent path: <root:list>[0]
            for cy in auto_builder_resource.auto_build_to_vfx_references:
                os_ = OS.LINUX
                run_config = RunConfig(
                    ops={
                        AssetKey([*ASSET_HEADER["key_prefix"], "rv_clone_write_dockerfile"]).to_python_identifier(): {
                            "config": {
                                "commit": commit_sha
                            }
                        },
                    },
                    resources={
                        "vfx_reference_platform_resource": VFXReferencePlatformResource(
                            cy=CY(cy),
                            os=os_,
                        ),
                    }
                )

                context.log.info(f"{ntfy_resource.ntfy_enable = }")

                if ntfy_resource.ntfy_enable:

                    auth = {}
                    if ntfy_resource.require_auth:
                        auth = {
                            "Authorization": get_base64_auth_str(
                                username=ntfy_resource.ntfy_username,
                                password=ntfy_resource.ntfy_password,
                                prefix="Basic",
                            ),
                        }

                    payload = json.dumps(
                        {
                            "topic": ntfy_resource.ntfy_topic,
                            "title": f"New {os_}-{cy} OpenRV Job",
                            "message": f"A new commit (`{commit_sha}`) was found on the AcademySoftwareFoundation/OpenRV repo. "
                                       f"A new build will be initiated for the VFX Reference Platform "
                                       f"{os_}-{cy} will be submitted.",
                            "tags": [
                                "new",
                            ],
                            "priority": 3,
                            "actions": [
                                {
                                    "action": "view",
                                    "label": "Open URL",
                                    "url": f"https://github.com/AcademySoftwareFoundation/OpenRV/commit/{commit_sha}",
                                },
                            ]
                        }
                    )

                    headers = {
                        "Content-Type": "application/json",
                        "Markdown": "yes",
                        **auth,
                    }

                    response_ntfy = requests.request(
                        method="POST",
                        url=ntfy_resource.ntfy_url,
                        headers=headers,
                        data=payload,
                    )

                    context.log.info(f"{response_ntfy.status_code = }")
                    context.log.debug(f"JSON response_ntfy: {json.dumps(response_ntfy.json(), indent=2, default=str)}")

                yield RunRequest(
                    run_key=run_key,
                    run_config=run_config,
                )

            context.log.info(f"Updating cursor...")
            # prepend instead of append. latest on top.
            previous_cursor.insert(0, commit_sha)
            new_cursor = json.dumps(previous_cursor)
            context.update_cursor(new_cursor)

            context.log.info(f"{context.cursor = }")

    else:
        yield SkipReason(f"GitHub API query limit reached. Maximum allowed: {rate_limit}. "
                         f"for more info, visit https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2026-03-10#checking-the-status-of-your-rate-limit.")
