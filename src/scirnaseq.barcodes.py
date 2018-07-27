#!/usr/bin/env python

"""
sciRNA-seq barcode parsing script.
"""

import os
import sys
from argparse import ArgumentParser
import numpy as np
import pandas as pd
import pysam
import time

__author__ = "Andre Rendeiro"
__copyright__ = "Copyright 2018, Andre Rendeiro"
__credits__ = []
__license__ = "GPL2"
__version__ = "0.2"
__maintainer__ = "Andre Rendeiro"
__email__ = "arendeiro@cemm.oeaw.ac.at"
__status__ = "Development"



def main():
    # Parse command-line arguments
    parser = ArgumentParser(
        prog="python scirnaseq.barcodes.py",
        description="\n".join([
            "sciRNA-seq script from Bock lab. " +
            "See https://github.com/epigen/open_pipelines and " +
            "https://github.com/epigen/open_pipelines/blob/master/" +
            "pipelines/sciRNAseq.barcodes.md for specific documentation. "])
    )
    parser = arg_parser(parser)
    # args = parser.parse_args("-a metadata/sciRNA-seq.oligos_2018-05-22v2.csv --mode slim samples_merged.bam".split(" "))
    args = parser.parse_args()
    print("# " + time.asctime() + " - Start.")
    print(args)

    annotation = pd.read_csv(args.annotation)

    # Get barcodes and annotate with mismatches
    cells = annotate_barcodes(
        extract_barcodes(args.input_file, start=args.start, end=args.end),
        annotation)

    # Slim down output if required
    cell_barcodes = ["round1", "round2", "round3a", "round3b"]
    if args.mode == "slim":
        # remove Ns
        cells = cells.loc[~(cells.loc[:, cells.columns[cells.columns.str.contains('_contains_N')]] == 1).any(axis=1), :]

        if args.max_mismatches > 0:
            for barcode in cell_barcodes:
                f = (
                    (cells[barcode + "_mismatches"] > 0) &
                    (cells[barcode + "_closest"] != "X") &
                    (cells[barcode + "_mismatches"] <= args.max_mismatches))
                cells.loc[f, barcode] = cells.loc[f, barcode + "_closest"]
                cells = cells.drop(barcode + "_closest", axis=1)

        # remove not matching any barcode with required mismatch threshold
        cells = cells.loc[
            ~(cells.loc[:, cells.columns[cells.columns.str.contains('_mismatches')]] > args.max_mismatches).any(axis=1), :]
        # remove unused column
        cells = cells.drop(cells.columns[cells.columns.str.contains('_')], axis=1)

    # Save
    o = ['read'] + cell_barcodes + ['umi']
    o += [x for x in cells.columns if x not in o]
    cells[o].sort_values("read").to_csv(args.output_file, index=False, compression="gzip")
    print("# " + time.asctime() + " - Done.")


def arg_parser(parser):
    """
    Global options.
    """
    parser.add_argument(
        dest="input_file",
        help="Input BAM file with reads to process.",
        type=str)
    parser.add_argument(
        "-a", "--annotation",
        dest="annotation",
        help="CSV file with barcode annotation.",
        type=str)
    default = "sciRNA-seq.barcodes.csv.gz"
    parser.add_argument(
        "-o", "--output",
        dest="output_file",
        help="Output file with barcodes. Default is '{}'.".format(default),
        default=default,
        type=str)
    choices = ["dark", "light"]
    parser.add_argument(
        "-m", "--method",
        dest="method",
        default=choices[0],
        choices=choices,
        help="sciRNA-seq method of the sample. " +
             "One of ['{}']. Default is '{}'.".format(
                "', '".join(choices), choices[0]),
        type=str)
    default = 0
    parser.add_argument(
        "--start",
        dest="start",
        help="Start line of BAM file to begin processing. " +
             "Default is first line (i.e. {}th line).".format(default),
        default=default,
        type=int)
    default = 1e100
    parser.add_argument(
        "--end",
        dest="end",
        help="End line of BAM file to finish processing. " +
             "Default is whole file (i.e. {}th  line)".format(default),
        default=default,
        type=int)
    parser.add_argument(
        "-d", "--dry-run",
        dest="dry_run",
        help="Whether not to actually do any work but just check files.",
        action="store_true")
    choices = ["fat", "slim"]
    parser.add_argument(
        "--mode",
        dest="mode",
        default=choices[1],
        choices=choices,
        help="Whether barcode correction should be applied and no record of original " +
             "barcode should be kept ('slim' mode), or all records should be kept in " +
             "the output file ('fat' mode). " +
             "One of ['{}']. Default is '{}'.".format(
                "', '".join(choices), choices[1]),
        type=str)
    default = 1
    parser.add_argument(
        "--max-mismatches",
        dest="max_mismatches",
        help="Maximum mismatches to allow correction. Default {}".format(default),
        default=default,
        type=int)

    return parser


def extract_barcodes(input_file, start=0, end=1e100):
    """
    """
    def reverse_complement(seq):
        """Reverse complement a DNA sequence.
        From https://stackoverflow.com/questions/25188968/reverse-complement-of-dna-strand-using-python

        :param seq: String to reverse complement.
        :type seq: str
        :returns: String reverse complemented.
        :rtype: str
        """
        alt_map = {'ins': '0'}
        complement = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A'} 
        for k,v in alt_map.items():
            seq = seq.replace(k,v)
        bases = list(seq) 
        bases = reversed([complement.get(base,base) for base in bases])
        bases = ''.join(bases)
        for k,v in alt_map.items():
            bases = bases.replace(v,k)
        return bases

    from collections import Counter
    input_handle = pysam.AlignmentFile(input_file, mode="rb", check_sq=False)

    errors = Counter()
    cells = list()
    i = 0
    print("# " + time.asctime() + " - Starting to extract barcodes.")
    for read in input_handle:
        i += 1

        if (start > (i - 1)):
            continue
        if (end < (i - 1)):
            break

        if (not read.is_paired) or read.is_read2:
            continue
        if (i - 1) % 1000000 == 0:
            print(i)
        umi = round1 = round2 = round3a = round3b = np.nan

        if len(read.seq) != 19:
            errors["read1_not_19bp"] += 1
        else:
            umi = read.seq[:8]
            round1 = read.seq[8:]

            # if round1 not in annoatation, set to nan
        if read.has_tag("BC"):
            read1 = dict(read.tags)["BC"]
            if len(read1) != 24:
                errors["readi7i5_not_24bp"] += 1
            else:
                read1_rc = reverse_complement(read1)
                round2 = read1_rc[16:]
                round3a = read1_rc[8:16]
                round3b = read1_rc[:8]
        else:
            errors["read1_not_BC_tag"] += 1

        cells.append([read.qname, round1, round2, round3a, round3b, umi])
        # cells.append([round1, round2, round3a, round3b, umi])

    print("# " + time.asctime() + " - Done extracting barcodes.")
    print("## Errors:")
    print(errors)
    return pd.DataFrame(data=cells, columns=["read", "round1", "round2", "round3a", "round3b", "umi"])


def annotate_barcodes(cells, annotation):
    def count_mismatches(a, b):
        return sum(a != b for a, b in zip(a, b))

    cell_barcodes = ["round1", "round2", "round3a", "round3b"]
    # fraction mapping to annotation
    print("# " + time.asctime() + " - Starting to annotate barcodes.")
    print("## Matching to annotation.")
    for a, barcode in enumerate(cell_barcodes):
        print(" - " + barcode)
        cells.loc[:, barcode + "_correct"] = (
            cells[barcode].isin(
                annotation.loc[annotation["barcode_type"] == barcode, "barcode_sequence"])).astype(int)
        cells.loc[:, barcode + '_contains_N'] = cells[barcode].str.contains("N").astype(int)

    print("# " + time.asctime() + " - Starting to correct barcodes.")
    for barcode in cell_barcodes[::-1]:
        print("## " + time.asctime() + " - " + barcode)
        ref = annotation.loc[annotation["barcode_type"] == barcode, "barcode_sequence"]
        print(" - Creating query")
        query = cells.loc[(cells[barcode + "_correct"] == 0) & (cells[barcode + "_contains_N"] == 0), barcode].drop_duplicates()
        print(" - Finding mismatches against reference")
        mis = pd.DataFrame([
            (q, count_mismatches(q, b), b)
            for b in ref
            for q in query], columns=[barcode, barcode + "_mismatches", barcode + '_closest'])
        print(" - " + time.asctime() + " -  Finding minimal match.")
        fix = mis.loc[mis.groupby(barcode)[barcode + "_mismatches"].idxmin()]
        print(" - " + time.asctime() + " -  Merging correct to reference.")
        cells = cells.set_index(barcode).join(fix.set_index(barcode)).reset_index()

        # have all entries with same type (no NAs)
        cells[barcode + "_mismatches"] = cells[barcode + "_mismatches"].fillna(0)
        cells[barcode + "_mismatches"] = cells[barcode + "_mismatches"].astype(int)
        cells[barcode + "_closest"] = cells[barcode + "_closest"].fillna("X")

    print("# Time: " + time.asctime() + " - Finished correcting barcodes.")
    return cells.sort_values("read")


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Program canceled by user!")
        sys.exit(1)