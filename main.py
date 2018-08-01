import click

from word_segmentation import segmentation

@click.group()
def clis():
    pass
clis.add_command(segmentation)

if __name__ == "__main__":
    clis()