# Andy Sayler
# Fall 2015
# From https://github.com/asayler/TorUniversity/blob/master/list.py

import click

def echo_table(values, headings=None, line_limit=None):

    # Process Args
    if line_limit is None:
        line_limit = click.get_terminal_size()[0]

    # Preprocess
    values = [[str(c) for c in r] for r in values]

    # Calculate lengths
    if headings:
        len_tab = ([headings] + values)
    else:
        len_tab = values
    lengths = []
    for row in len_tab:
        for c in range(len(row)):
            if len(lengths) > c:
                if len(row[c]) > lengths[c]:
                    lengths[c] = len(row[c])
            else:
                lengths.append(len(row[c]))

    # Set Max Lengths
    if line_limit:
        # Calculate Max Lengths
        while sum(lengths) + (len(lengths) * 3) > line_limit:
            lengths[lengths.index(max(lengths))] -= 1
            if max(lengths) <= 4:
                break
        # Truncate Headings
        if headings:
            for c in range(len(headings)):
                if len(headings[c]) > lengths[c]:
                    headings[c] = headings[c][:(lengths[c]-3)] + "..."
        # Truncate Values
        for row in values:
            for c in range(len(row)):
                if len(row[c]) > lengths[c]:
                    row[c] = row[c][:(lengths[c]-3)] + "..."

    # Print Headings
    if headings:
        for c in range(len(lengths)):
            if c < len(headings):
                click.echo("{val:^{width}s} | ".format(val=headings[c], width=lengths[c]), nl=False)
            else:
                click.echo("{val:^{width}s} | ".format(val="", width=lengths[c]), nl=False)
        click.echo("")
        for c in range(len(lengths)):
            click.echo("{val:{fill}^{width}s} | ".format(val='-', fill='-', width=lengths[c]), nl=False)
        click.echo("")

    # Print Table
    for row in values:
        for c in range(len(lengths)):
            if c < len(row):
                click.echo("{val:<{width}s} | ".format(val=row[c], width=lengths[c]), nl=False)
            else:
                click.echo("{val:<{width}s} | ".format(val="", width=lengths[c]), nl=False)
        click.echo("")
