from tools.crawler import crawl
from tools.visualizer import visualize
import sys
import yaml


def load_config(file):
    with open("config/"+file, 'r') as stream:
        links = yaml.safe_load(stream)
        return links

if __name__ == "__main__":
    assert sys.argv[1] == "crawl", 'You mean "crawl"?'
    assert sys.argv[2] in ("--file", "-F"), 'Should be "--file" or "-F"'
    assert sys.argv[3].endswith(('.yml', ".yaml")) , "File should be YML format!"
    
    links = load_config(sys.argv[3])

    assert links != None, f"No key in {sys.argv[3]}."
    for link in links:
        try:
            print(f"Crawling {link}: ")
            assert crawl(link) != False, "\tlink is not valid or shopee has been shutdown\n"
        except AssertionError as msg:
            print(msg)
            continue
            