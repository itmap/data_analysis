import click
import logging

#from word_segment import segmentation
from text_rank import textrank

logging.basicConfig(level=logging.INFO)

@click.group()
def clis():
    pass


#clis.add_command(segmentation)
clis.add_command(textrank)

if __name__ == "__main__":
    clis()
