import os
import time
import pickle
import threading
from queue import Queue
from pathlib import Path
import dataiku
import yaml
from dataikuapi import DSSClient
from dataikuapi.dss.projectfolder import DSSProjectFolder
from dataikuapi.dss.project import DSSProject
from dataikuapi.dss.recipe import CodeRecipeSettings, DSSRecipeListItem, DSSRecipe
from dataikuapi.dss.job import DSSJobWaiter
from typing import List, Dict

SETTINGS_FILE = os.path.join(os.path.expanduser("~"), "dataiku-tools-settings.yml")
DSS_INSTANCE = "https://dataiku-ops-pp.aiml.gcp.levi.com"


def get_api_secret() -> str | None:
    if not os.path.exists(SETTINGS_FILE):
        Path(SETTINGS_FILE).touch()

    with open(SETTINGS_FILE, "r") as stream:
        try:
            settings = yaml.safe_load(stream)

            if type(settings) != dict:
                return None
            return settings["api-key"]
        except yaml.YAMLError as exc:
            return None


def start_client(apiKey: str) -> DSSClient:
    dataiku.set_remote_dss(DSS_INSTANCE, apiKey)
    client = dataiku.api_client()
    info = client.get_auth_info()
    print(info)
    return client

def collect_project_keys(folder: DSSProjectFolder) -> set[str]:
    child_folders = folder.list_child_folders()
    keys = set(folder.list_project_keys())
    if (len(child_folders) == 0):
        return keys
    else:
        return keys.union(*[collect_project_keys(child) for child in child_folders])

def collect_recipes(project: DSSProject) -> List[DSSRecipe]:
    recipes_to_run = []
    recipes = project.list_recipes()
    for recipe in recipes:

        if type(recipe) is DSSRecipeListItem:
            recipe = recipe.to_recipe()

        recipe_settings = recipe.get_settings()

        if type(recipe_settings) is CodeRecipeSettings:
            recipes_to_run.append(recipe)
        else:
            raw_settings = recipe_settings.get_recipe_raw_definition()
            if raw_settings.get('params', {}).get('engineType') == 'SPARK':
                print(f'SPARK {recipe.name}')
                recipes_to_run.append(recipe)                
                
    return recipes_to_run


def run_recipe(project: DSSProject, recipe: DSSRecipe, queue: Queue):
    print(f'Staring {project.project_key}, {recipe.name}')
    job = recipe.run(no_fail=True)
    result = str(DSSJobWaiter(job).wait(no_fail=True))
    queue.put(f'{project.project_key},{recipe.name},{result}\n')
    print(f'Completed {project.project_key}, {recipe.name}, {result}')

def execute_recipes(recipes: Dict[DSSProject, List[DSSRecipe]]):
    results_queue = Queue()
    threads = []
    for project, recipes in recipes.items():
        for recipe in recipes:
            if len(threads) == 5:
                placed = False
                while not placed:
                    for index, thread in enumerate(threads):
                        if not thread.is_alive():
                            thread.join()
                            while not results_queue.empty():
                                result = results_queue.get()
                                with open('results.csv', 'a+') as f:
                                    f.write(result)
                            new_thread = threading.Thread(target=run_recipe, args=(project, recipe, results_queue))
                            new_thread.start()
                            threads[index] = new_thread
                            placed = True
                            break
                    time.sleep(10)
            else:
                new_thread = threading.Thread(target=run_recipe, args=(project, recipe, results_queue))
                new_thread.start()
                threads.append(new_thread)

    if len(threads) > 0:
        for thread in threads:
            thread.join()
        while not results_queue.empty():
            result = results_queue.get()
            with open('results.csv', 'a+') as f:
                f.write(result)

if __name__ == '__main__':
    client = start_client(get_api_secret())

    try:
        with open('recipes', 'rb') as file:
            recipes_to_run = pickle.load(file)
    except:
        recipes_to_run = None
    
    if recipes_to_run is None:
        project_keys = set()
        recipes_to_run: Dict[DSSProject, List[DSSRecipe]] = {}
        

        mpim_folder: None | DSSProjectFolder  = None
        for folder in client.get_root_project_folder().list_child_folders():
            if folder.name == 'LSE MPIM':
                mpim_folder = folder

        if mpim_folder is None:
            print('No MPIM folder found')
            exit()

        project_keys = collect_project_keys(mpim_folder)

        print(f'Found {len(project_keys)} projects...')
        for project_key in project_keys:
            print(project_key)
            project = client.get_project(project_key)
            recipes = collect_recipes(project)
            recipes_to_run[project] = recipes
            print('############\n')

        with open('recipes', 'wb') as file:
            pickle.dump(recipes_to_run, file)
    else:
        execute_recipes(recipes_to_run)

    

