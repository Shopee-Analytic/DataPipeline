from tools.crawler import crawl
from tools.visualizer import visualize
import sys
import yaml

def is_int(val):
    try:
        num = int(val)
    except ValueError:
        return False
    return True

def load_config(file):
    with open("config/"+file, 'r') as stream:
        links = yaml.safe_load(stream)
        return links

if __name__ == "__main__":
    assert sys.argv[1] in ("crawl", "visualize"), '"crawl" or "visualize" should be call'

    if sys.argv[1] == "crawl":
        assert sys.argv[2] in ("--input", "-I"), 'Should be "--input" or "-I".'
        assert sys.argv[3].endswith(('.yml', ".yaml")) , "File should named by 'file_name.yml'."
        assert sys.argv[4] in ("--ouput", "-O"), 'Should be "--output" or "-0".'
        assert sys.argv[5].endswith(".json"), 'File should be named by "file_name.json".'

        links = load_config(sys.argv[3])
        assert links != None, f"No key in {sys.argv[3]}."
        for link in links:
            try:
                print(f"Crawling {link}: ")
                assert crawl(link, sys.argv[5]) != False, "\tlink is not valid or shopee has been shutdown\n"
            except AssertionError as msg:
                print(msg)
                continue

    elif sys.argv[1] == "visualize":
        assert sys.argv[2] in ("--file", "-F"), 'Should be "--file" or "-F".'
        assert sys.argv[3].endswith(".json"), 'File should be named by "file_name.json".'
        assert is_int(sys.argv[4]), "Number of product should be an integer"
        
        visualize(sys.argv[3], sys.argv[4])
            