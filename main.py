from tools.crawler import crawl_to_file
from tools.visualizer import visualize
from tools.worker import start_crawling
import sys
import yaml
from os import path, mkdir, getcwd, system
from tools import scheduler
def load_config(file):
    with open("config/"+file, 'r') as stream:
        links = yaml.safe_load(stream)
        return links

if __name__ == "__main__":
    if not path.exists("config"):
        mkdir(path.join(getcwd(), 'config'))
    if not path.exists("log"):
        mkdir(path.join(getcwd(), "log"))

    assert sys.argv[1] in ("crawl", "crawl_to_file", "visualize", "scheduler"), '"crawl" or "crawl_to_file" or "visualize" should be called'

    if sys.argv[1] == "crawl_to_file":
        assert sys.argv[2] in ("--input", "-I"), 'Should be "--input" or "-I".'
        assert sys.argv[3].endswith(('.yml', ".yaml")) , "File should be named by 'file_name.yml'."
        assert path.exists('config/{}'.format(sys.argv[3])), "File not existed"
        assert sys.argv[4] in ("--output", "-O"), 'Should be "--output" or "-0".'
        assert sys.argv[5].endswith(".json"), 'File should be named by "file_name.json".'
        links = load_config(sys.argv[3])
        assert links != None, f"No key in {sys.argv[3]}."
        if not path.exists("data"):
            mkdir(path.join(getcwd(), 'data'))
        for link in links:
            try:
                print(f"Crawling {link}: ")
                assert crawl_to_file(link, sys.argv[5]) == True, "\tlink is not valid or shopee has been shutdown\n"
            except AssertionError as msg:
                print(msg)
                continue

    elif sys.argv[1] == "visualize":
        assert sys.argv[2] in ("--file", "-F"), 'Should be "--file" or "-F".'
        assert sys.argv[3].endswith(".json"), 'File should be named by "file_name.json".'
        assert path.exists('data/{}'.format(sys.argv[3]))
        try:
            assert int(sys.argv[4]), "Number of product should be an integer.\n"
            length = int(sys.argv[4])
        except AssertionError as msg:
            print(msg)
            length=0
        except IndexError:
            length=1

        visualize(sys.argv[3], length)
    
    elif sys.argv[1] == "crawl":
        
        assert sys.argv[2] in ("--input", "-I"), 'Should be "--input" or "-I".'
        assert sys.argv[3].endswith(('.yml', ".yaml")) , "File should be named by 'file_name.yml'."
        try:
            assert int(sys.argv[4]), "Number of page should be an integer.\n"
            number_of_page=int(sys.argv[4])
        except AssertionError as msg:
            print(msg)
        except IndexError:
            number_of_page=1
        links = load_config(sys.argv[3])
        print("Links in list:")
        for link in links:
            print(f"{link}")
        start_crawling(links)

    elif sys.argv[1] == "scheduler":
        assert sys.argv[2] in ("add_job", "run", "remove_all", "show"), 'Should be "add_job", "run" or "remove_all".'
        if sys.argv[2] == "add_job":
            scheduler.add_job()
        elif sys.argv[2] == "run":
            scheduler.run()
        elif sys.argv[2] == "remove_all":
            scheduler.remove_all()
        elif sys.argv[2] == "show":
            scheduler.show_all()