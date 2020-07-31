import click
import csv
import boto3
import re

from botocore import UNSIGNED
from botocore.client import Config


def _parse_s3_uri(s3_uri):
    """Parse an Amazon S3 URI into bucket and key entities

    Parameters
    ----------
    s3_uri : str
        The Amazon S3 URI

    Returns
    -------
    dict
        dict with keys "bucket" and "key"
    """
    # Parse s3_uri into bucket and key
    pattern = r"(s3:\/\/)?(?P<bucket>[^\/]*)\/(?P<key>.*)"
    m = re.match(pattern, s3_uri)
    if m is None:
        raise ValueError(
            "s3_uri is not a valid URI. It should match the regex pattern {pattern}. You provided {s3_uri}.".format(
                pattern=pattern, s3_uri=s3_uri
            )
        )
    else:
        return m.groupdict()


def _get_s3_client(anon=True):
    """Return a boto3 s3 client

    Global boto clients are not thread safe so we use this function
    to return independent session clients for different threads.

    Parameters
    ----------
    anon : bool, default=True
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order)

    Returns
    -------
    s3_client : boto3.client('s3')
    """
    session = boto3.session.Session()
    if anon:
        s3_client = session.client("s3", config=Config(signature_version=UNSIGNED))
    else:
        s3_client = session.client("s3")

    return s3_client


def get_matching_s3_keys(s3_uri, suffix="", anon=True, add_version=True):
    """Generate all the matching keys in an S3 bucket.

    Parameters
    ----------
    s3_uri : str
        The Amazon S3 URI

    suffix : str, optional
        Only fetch keys that end with this suffix

    anon : bool, default=True
        Whether to use anonymous connection (public buckets only).
        If False, uses the key/secret given, or boto’s credential
        resolver (client_kwargs, environment, variables, config files,
        EC2 IAM server, in that order)

    add_version : bool, default=True
        If True, return a tuple of (<S3 key>, <version>), otherwise
        just return <S3 key>

    Yields
    ------
    key : list
        S3 keys that match the prefix and suffix
    """
    s3 = _get_s3_client(anon=anon)
    parsed = _parse_s3_uri(s3_uri=s3_uri)
    bucket = parsed["bucket"]
    prefix = parsed["key"]

    kwargs = {"Bucket": bucket, "MaxKeys": 1000}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str) and prefix:
        kwargs["Prefix"] = prefix

    while True:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp["Contents"]
        except KeyError:
            return

        for obj in contents:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(suffix):
                output = "/".join(["s3:/", bucket, key])
                if add_version:
                    yield output, obj["ETag"].strip('"')
                else:
                    yield output

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except KeyError:
            break


@click.command()
@click.option(
    "--anon/--no-anon", default=True, help="whether to use an anonymous S3 connection",
)
@click.option(
    "--dataset_name",
    default=None,
    type=str,
    help="dataset name to be included in csv file",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    help="whether to overwrite any pre-existing csv file",
)
@click.argument("s3_uri")
@click.argument("csv_path", type=click.Path())
def create_add_urls_csv(s3_uri, csv_path, anon, dataset_name, overwrite):
    """Create a csv for ingestion into datalad addurls

    Arguments:

    S3_URI is the Amazon S3 URI

    CSV_PATH is the path to the csv to be written
    """
    matching_keys = get_matching_s3_keys(s3_uri=s3_uri, anon=anon, add_version=True)

    parsed = _parse_s3_uri(s3_uri)
    dataset_name = dataset_name if dataset_name is not None else parsed["key"]
    split_key = parsed["key"]

    mode = "w" if overwrite else "x"

    with open(csv_path, mode=mode) as urls_csv:
        csv_writer = csv.writer(
            urls_csv, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL
        )
        csv_writer.writerow(["original_url", "dataset", "filename", "version"])

        for s3_url, version in matching_keys:
            original_url = s3_url
            filename = s3_url.split(split_key + "/")[-1].replace("/", "//")
            csv_writer.writerow([original_url, dataset_name, filename, version])


if __name__ == "__main__":
    create_add_urls_csv()
