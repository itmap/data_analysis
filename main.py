import click

from word_segmentation import segmentation
from workers import entrance

@click.group()
def clis():
    pass
clis.add_command(segmentation)
clis.add_command(entrance)

if __name__ == "__main__":
    clis()
