import click
import logging

#from word_segment import segmentation

logging.basicConfig(level=logging.INFO)

@click.group()
def clis():
    pass


#clis.add_command(segmentation)

if __name__ == "__main__":
    clis()
