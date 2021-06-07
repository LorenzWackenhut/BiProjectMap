import time
import requests
import json
from os import walk, path
import pandas as pd
import random
from time import sleep
from functools import wraps
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
import yaml
from datetime import datetime, time
import geopandas as gpd
import shapely
from shapely import wkb
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt


def retry(num_of_times=3):
    """Decorator that catches exceptions from a given function and retries to execute n times"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(num_of_times):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    return_exc = e
                    print(f"Retrying...")
                    sleep(10)
            raise return_exc

        return wrapper

    return decorator


class Coordinates:
    """Class bundles all variables and methods for extracting and sampling of coordinates"""

    def __init__(
        self,
        path_file,
        geo_barrier,
        city_size=None,
        city=None,
        location_randomization=0.05,
    ):
        self.data = self._get_data(path_file, city_size, city)
        self.location_randomization = location_randomization
        self.geo_barrier = geo_barrier

    def _get_data(self, path_file, city_size, city):
        """Extracts the coordinates from a given data source"""
        data = pd.read_csv(path_file, sep=None, engine="python")
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

        def randomize_locations(data):
            """Randomizes the location by adding/substracting a random value"""

            def randomize(df, col, rand_low, rand_high):
                df[col] = (
                    df[col]
                    .apply(
                        lambda x: str(
                            float(x.replace(",", "."))
                            + random.uniform(rand_low * (-1), rand_high)
                        )
                    )
                    .astype("float")
                )
                return df

            rand_low = self.location_randomization
            rand_high = self.location_randomization
            if self.geo_barrier["lon_neg"]:
                rand_low = 0
            if self.geo_barrier["lon_pos"]:
                rand_high = 0
            data = randomize(data, "Longitude", rand_low, rand_high)
            if self.geo_barrier["lat_neg"]:
                rand_low = 0
            if self.geo_barrier["lat_pos"]:
                rand_high = 0
            data = randomize(data, "Latitude", rand_low, rand_high)
            return data

        sample = self.data.sample(num_samples, replace=True)
        sample = randomize_locations(sample)
        coordinates = sample[["Longitude", "Latitude"]]
        return coordinates.to_numpy().tolist(), coordinates


class Jsons:
    """Class bundles methods to create a sample of json posts"""

    def __init__(
        self,
        coordinates,
        identifier,
        copy_ratio=0.5,
        os="ios",
        devices="pn-message-token",
        softwareversions="sa-1.0.10",
    ):
        self.coordinates = coordinates
        self.identifier = identifier
        self.copy_ratio = copy_ratio
        self.os = os
        self.device = devices
        self.softwareversions = softwareversions

    def create_jsons(self):
        """Creates a sample of json files with specifies attributes"""
        choice_pop = ["original", "copy"]
        choice_weights = [1 - self.copy_ratio, self.copy_ratio]
        sample_jsons = []
        for x in range(len(self.coordinates)):
            sample_json = {
                "identifier": self.identifier,
                "copyclassification": random.choices(choice_pop, choice_weights, k=1)[
                    0
                ],
                "location": {
                    "latitude": self.coordinates[x][1],
                    "longitude": self.coordinates[x][0],
                },
                "data": {"key": "value"},
                "softwareInfo": {
                    "os": self.os,
                    "device": self.device,
                    "softwareversion": self.softwareversions,
                },
            }
            sample_jsons.append(sample_json)
        return sample_jsons


class Post:
    """Encapsulates all methods for posting the json samples including paralell threading"""

    def __init__(
        self,
        sample_jsons,
        auth="""eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhdXRoZW50aWMiLCJleHAiOjE4OTM0NTIzOTksImtleWlkIjoiUmh5YmtnY1hha1VpUWMyWmhQa1UiLCJyb2xlcyI6WyJwcm9kdWNlciJdfQ.z_p5-75_grgnumjiOXV-PUkAkM_q6dMNVl9yCqC1oChqhG5fqTNEtEgQ5mfag0XJRmHx7o5dRs-lpM61jUhZXA""",
        url="https://api.authentic.network/api/v2/scans",
    ):
        self.sample_jsons = sample_jsons
        self.url = url
        self.auth = auth

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
        if num_posts < 5:
            num_threads = num_posts
        else:
            num_threads = 5
        json_chunks = chunks(self.sample_jsons, num_threads)

        @retry(100)
        def post_request(chunk):
            """Iterates through chunks and posts jsons individually"""
            for i, sample in enumerate(chunk):
                # print(sample)
                sample_dump = json.dumps(sample)
                headers = {
                    "content-type": "application/json",
                }
                response = requests.post(self.url, data=sample_dump, headers=headers)
                if response.ok:
                    print(f"Successfully posted json #{i+1}\n")
                else:
                    print(f"Unable to post json #{i+1}")
                    print(response)
                    raise Exception("Connection to REST API terminated abnormally")

        def runner(json_chunks, num_threads):
            """Manages threads for parallelisation"""
            threads = []
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                for chunk in json_chunks:
                    # thread_name = uuid.uuid1()
                    threads.append(executor.submit(post_request, chunk))

        runner(json_chunks, num_threads)


class Map:
	"""Creates a map visualization of germany with point coordinates plotted"""
	
    def __init__(self, path_shape, geo_projection="EPSG:4326"):
        self.path_shape = path_shape
        self.geo_projection = geo_projection
        self._read_shapefile()

    def _read_shapefile(self):
        self.shapefile = gpd.read_file(self.path_shape, crs=self.geo_projection)

    def plot_points(self, df, save_fig=False):
        self.df = df
        self._transform_df()
        self._create_map(save_fig)

    def _transform_df(self):
        self.df["xy"] = [
            Point(xy) for xy in zip(self.df["Longitude"], self.df["Latitude"])
        ]
        self.df = gpd.GeoDataFrame(self.df, crs=self.geo_projection)

    def _create_map(self, save_fig):
        plt.figure()
        fig, ax = plt.subplots(figsize=(40, 40))
        self.shapefile.plot(ax=ax, color="white", alpha=0.8)
        for i, row in self.df.iterrows():
            ax.plot(row["xy"].x, row["xy"].y, marker="o", c="red", alpha=0.5)
        ax.set(title="Germany", aspect=1.3, facecolor="lightblue")
        plt.show()
        if save_fig:
            plt.savefig("map_all_points.png", dpi=600)


def main():
    def build_path(name_file):
	"""Builds a relative path"""
	
        path_script = path.dirname(path.abspath(__file__))
        return path.join(path_script, name_file)

    def load_config(path_config):
	"""Loads the yaml config"""
	
        path_config_full = build_path(path_config)
        with open(path_config_full) as f:
            config = yaml.load(f, Loader=yaml.FullLoader)
        return config

    def wait_until(start_time):
	"""Sleeps the script until a certain time"""
	
        start_time = time(*(map(int, start_time.split(":"))))
        while start_time > datetime.today().time():
            print(f"Waiting until: {start_time}")
            sleep(60)

    config = load_config("config_runs.yml")
    path_file = build_path(config["coordinates_name_file"])

    runs = [value for key, value in config.items() if "run" in key.lower()]
    df_coordinates = pd.DataFrame()
    for i, run in enumerate(runs):
        print(f"Initiated run #{i+1}")

        if config["wait_time"]:
            wait_until(run["time"])
            print("Starting")

        coordinates = Coordinates(
            path_file,
            run["geo_barrier"],
            run["city_types"],
            run["city_names"],
            run["location_randomization"],
        )
        sample_coordinates, df_coordinates_run = coordinates.get_coordinates(
            run["num_samples"]
        )

        if config["post_to_api"]:
            jsons = Jsons(sample_coordinates, identifier=config["identifier"])
            sample_jsons = jsons.create_jsons()
            api_post = Post(sample_jsons)
            api_post.post_jsons()

        df_coordinates = pd.concat([df_coordinates, df_coordinates_run])

    if config["show_map"]:
        map_ger = Map(build_path(config["shapefile"]))
        map_ger.plot_points(df_coordinates, config["save_map"])


if __name__ == "__main__":
    main()
