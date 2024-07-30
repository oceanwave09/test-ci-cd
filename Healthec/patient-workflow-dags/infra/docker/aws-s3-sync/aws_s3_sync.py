import hashlib
import os
from urllib.parse import urlparse

import boto3
import click


def md5_checksum(filename):
    m = hashlib.md5()
    with open(filename, "rb") as f:
        for data in iter(lambda: f.read(1024 * 1024), b""):
            m.update(data)

    return m.hexdigest()


def etag_checksum(filename, chunk_size=8 * 1024 * 1024):
    with open(filename, "rb") as f:
        if os.path.getsize(filename) >= chunk_size:
            md5s = []
            for data in iter(lambda: f.read(chunk_size), b""):
                md5s.append(hashlib.md5(data).digest())
            m = hashlib.md5(b"".join(md5s))
        else:
            m = hashlib.md5(f.read())
    return "{}-{}".format(m.hexdigest(), len(md5s))


def etag_compare(filename, etag):
    et = etag[1:-1]  # strip quotes
    if "-" in et and et == etag_checksum(filename):
        return True
    if "-" not in et and et == md5_checksum(filename):
        return True
    return False


def download_dir(bucket, prefix, local):
    client = boto3.client("s3")

    def create_folder_and_download_file(key):
        dest_pathname = os.path.join(local, key.replace(prefix, "", 1).lstrip("/"))
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))

        # download the file only if it does not exists or checksum not matching
        if not os.path.isfile(dest_pathname):
            # print(f"downloading {key} to {dest_pathname}")
            client.download_file(bucket, key, dest_pathname)
        else:
            object = client.get_object(Bucket=bucket, Key=key)
            etag = object.get("ETag", "")
            if not etag_compare(dest_pathname, etag):
                # print(f"checksums are not matching, downloading {key} to {dest_pathname}")
                client.download_file(bucket, key, dest_pathname)
        return dest_pathname

    def cleanup_folder(keys):
        s3_files = [key.replace(prefix, "", 1).lstrip("/") for key in keys]
        # print(f"s3 files : {s3_files}")
        for path, _, files in os.walk(local):
            # print(f"path : {path}")
            for name in files:
                # print(f"name : {name}")
                local_file = os.path.join(path, name).replace(local, "", 1).lstrip("/")
                # print(f"local file : {local_file}")
                if local_file not in s3_files:
                    os.remove(os.path.join(local, local_file))

    keys = []
    dirs = []
    next_token = ""
    base_kwargs = {
        "Bucket": bucket,
        "Prefix": prefix,
    }
    while next_token is not None:
        kwargs = base_kwargs.copy()
        if next_token != "":
            kwargs.update({"ContinuationToken": next_token})
        results = client.list_objects_v2(**kwargs)
        contents = results.get("Contents")
        for i in contents:
            k = i.get("Key")
            if k[-1] != "/":
                keys.append(k)
            else:
                dirs.append(k)
        next_token = results.get("NextContinuationToken")
    for d in dirs:
        dest_pathname = os.path.join(local, d.replace(prefix, "", 1).lstrip("/"))
        if not os.path.exists(os.path.dirname(dest_pathname)):
            os.makedirs(os.path.dirname(dest_pathname))
    cleanup_folder(keys)
    # with futures.ThreadPoolExecutor() as executor:
    #     futures.wait(
    #         [executor.submit(create_folder_and_download_file, key) for key in keys],
    #         return_when=futures.ALL_COMPLETED,
    #     )
    for key in keys:
        create_folder_and_download_file(key)


@click.command()
@click.option("--s3-path", help="source s3 path", default=None)
@click.option("--dest-dir", help="local destination directory", default=None)
def main(s3_path, dest_dir):
    print("calling s3 path sync with local...")
    # parse the source s3 path and get bucket name and key path
    parsed_results = urlparse(s3_path)

    # apply s3 sync
    download_dir(parsed_results.netloc, parsed_results.path.lstrip("/"), dest_dir)
    print("s3 path sync completed")


if __name__ == "__main__":
    main()
