import os
import json
import boto3
import botocore.exceptions

"""
For not caring about having aws profiles been managed while testing code locally
and while sharing code to others or to aws resource eg: lambda, ec2, ecs
"""
class botoSession:

    def __init__(self):
        self.profile_name:str = ""
        self.region_name:str  = ""

    """
    search for aws profile configs in json file present at `conf_path`
    for boto3 sesstion to consider profile name defined in that json file
    """
    def config(self, conf_path: str = ""):
        if os.path.exists(conf_path):
            with open(f'{conf_path}', 'r') as c:
                conf = json.loads(c.read())
            self.profile_name = conf.get("aws_profile_name") or ""
            self.region_name = conf.get("aws_region_name") or ""

    """
    check for boto3 Session profile, if not present, set to default
    """
    def session(self, profile_name: str = "", region_name: str = ""):
        if self.profile_name != "":
            pass
        elif profile_name != "":
            self.profile_name = profile_name
            self.region_name = region_name
        else:
            return boto3.Session()

        try:
            if self.region_name != '':
                return boto3.Session(
                    profile_name=self.profile_name,
                    region_name=self.region_name
                )
            else:
                return boto3.Session(profile_name=self.profile_name)

        except botocore.exceptions.ProfileNotFound:
            return boto3.Session()
