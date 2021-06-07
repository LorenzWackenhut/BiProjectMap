import time
import requests
import json
from os import walk, path
import pandas as pd
import random
from functools import wraps
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml


class Coordinates:
    """Class bundles all variables and methods for extracting and sampling of coordinates"""

    def __init__(self, path_file, city_size=None, city=None):
        self.data = self._get_data(path_file, city_size, city)

    def _get_data(self, path_file, city_size, city):
        """Extracts the coordinates from a given data source"""
        data = pd.read_excel(path_file)
        if city_size is not None:
            if isinstance(city_size, list):
                data = data[data["Untersegment"].isin(city_size)]
            else:
                data = data[data["Untersegment"] == city_size]
        if city is not None:
            if isinstance(city, list):
                data = data[data["Gemeinde"].isin(city)]
            else:
                data = data[data["Gemeinde"] == city]
        return data

    def get_coordinates(self, num_samples):
        """Returns a sample of coordinates with a specified size"""
        sample = self.data.sample(num_samples, replace=True)
        coordinates = sample[["Longitude", "Latitude"]]
        return coordinates.to_numpy().tolist()


class Jsons:
    """Class bundles methods to create a sample of json posts"""

    def __init__(
        self,
        coordinates,
        identifiers=tuple("0008 0023 002c 0037 0050 005c 0063 0009 0031 0042".split()),
        copyclassifications_original=1,
        copyclassifications_copy=0,
        os="ios",
        devices="pn-message-token",
        softwareversions="sa-1.0.10",
    ):
        self.coordinates = (coordinates,)
        self.identifiers = (identifiers,)
        self.copyclassifications_original = (copyclassifications_original,)
        self.copyclassifications_copy = (copyclassifications_copy,)
        self.os = (os,)
        self.device = (devices,)
        self.softwareversions = softwareversions

    def create_jsons(self):
        """Creates a sample of json files with specifies attributes"""
        sample_jsons = []
        for x in range(len(self.coordinates[0])):
            sample_json = {
                "identifier": random.choice(self.identifiers[0]),
                "copyclassification": "original",
                "location": {
                    "latitude": self.coordinates[0][x][1],
                    "longitude": self.coordinates[0][x][0],
                },
                "data": {"key": "value"},
                "os": self.os,
                "device": self.device,
                "softwareversion": self.softwareversions,
            }
            sample_jsons.append(sample_json)
        return sample_jsons


class Post:
    """Encapsulates all methods for posting the json samples including paralell threading"""

    def __init__(
        self, sample_jsons, url="http://127.0.0.1:5000/test",
    ):
        self.sample_jsons = sample_jsons
        self.url = url

    def post_jsons(self):
        """Posts the json samples to the REST API with a specified rate"""

        def chunks(sample_json, n):
            """Devides the samples into equal parts to allow parallelisation"""
            avg = len(sample_json) / float(n)
            json_chunks = []
            last = 0.0
            while last < len(sample_json):
                json_chunks.append(sample_json[int(last) : int(last + avg)])
                last += avg
            return json_chunks

        num_posts = len(self.sample_jsons)
        if num_posts < 20:
            num_threads = num_posts
        else:
            num_threads = 20
        json_chunks = chunks(self.sample_jsons, num_threads)

        def post_request(chunk):
            """Iterates through chunks and posts jsons individually"""
            for i, sample in enumerate(chunk):
                sample_dump = json.dumps(sample)
                headers = {
                    "content-type": "application/json",
                }
                requests.post(self.url, data=sample_dump, headers=headers)
                print(f"Posted JSON #{i+1}")

        def runner(json_chunks, num_threads):
            """Manages threads for parallelisation"""
            threads = []
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                for chunk in json_chunks:
                    # thread_name = uuid.uuid1()
                    threads.append(executor.submit(post_request, chunk))

        runner(json_chunks, num_threads)


def main():
    def build_path(name_file):
        path_script = path.dirname(path.abspath(__file__))
        return path.join(path_script, name_file)

    def load_config(path_config):
        path_config_full = build_path(path_config)
        with open(path_config_full) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return config

    config = load_config("config.yml")

    path_file = build_path(config["coordinates_name_file"])
    coordinates = Coordinates(path_file, config["city_types"], config["city_names"])
    sample_coordinates = coordinates.get_coordinates(config["num_samples"])

    jsons = Jsons(sample_coordinates)
    sample_jsons = jsons.create_jsons()

    api_post = Post(sample_jsons)

    start_time_all = time.time()
    api_post.post_jsons()
    total_time = time.time() - start_time_all
    print(f"Total time: {total_time}")
    print(f"Average time per post: {total_time / config['num_samples']}")


if __name__ == "__main__":
    main()
