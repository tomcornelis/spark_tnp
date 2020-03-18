#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import argparse
import getpass
from tqdm.auto import tqdm

from muon_definitions import get_allowed_resonances, get_allowed_eras


# parallel processing
def _futures_handler(futures_set, status=True, unit='items', desc='Processing',
                     add_fn=None, output=None):
    from tqdm.auto import tqdm
    import time

    try:
        with tqdm(disable=not status, unit=unit, total=len(futures_set),
                  desc=desc, ncols=80) as pbar:
            while len(futures_set) > 0:
                finished = set(job for job in futures_set if job.done())
                futures_set.difference_update(finished)
                while finished:
                    res = finished.pop().result()
                    if add_fn:
                        add_fn(output, res)
                    pbar.update(1)
                time.sleep(0.5)
    except KeyboardInterrupt:
        for job in futures_set:
            job.cancel()
    except Exception:
        for job in futures_set:
            job.cancel()
        raise


# argparse common functions
def add_common_multi(parser):
    parser.add_argument('--workers', '-j', type=int, default=1,
                        help='Number of cores')
    parser.add_argument('--dryrun', action='store_true',
                        help='Don\'t run, just print number of jobs')
    parser.add_argument('--condor', action='store_true',
                        help='Prepare condor submit script')


def add_common_flatten(parser):
    parser.add_argument('--numerator', nargs='*',
                        help='Filter by numerator')
    parser.add_argument('--denominator', nargs='*',
                        help='Filter by denominator')
    parser.add_argument('--shiftType', nargs='*',
                        help='Filter by shift type')


def add_common_fit(parser):
    parser.add_argument('--numerator', nargs='*',
                        help='Filter by numerator')
    parser.add_argument('--denominator', nargs='*',
                        help='Filter by denominator')
    parser.add_argument('--fitType', nargs='*',
                        help='Filter by fit type')
    parser.add_argument('--shiftType', nargs='*',
                        help='Filter by shift type')
    parser.add_argument('--sampleType', nargs='*',
                        help='Filter by sample type (data, mc)')
    parser.add_argument('--efficiencyBin', nargs='*',
                        help='Filter by efficiency bin')


def add_common_particle(parser):
    parser.add_argument('particle', choices=['muon', 'electron'],
                        help='Particle for scalefactors')


def add_common_resonance(parser):
    allowed = get_allowed_resonances()
    parser.add_argument('resonance', choices=allowed,
                        help='Resonance for scalefactors')


def add_common_era(parser):
    a = get_allowed_resonances()
    allowed = []
    for r in a:
        allowed += get_allowed_eras(r)
    allowed = set(allowed)
    parser.add_argument('era', choices=allowed,
                        help='Scale factor set to produce')


def add_common_options(parser):
    parser.add_argument('--baseDir', default='',
                        help='Working directory')


def parse_command_line(argv):
    parser = argparse.ArgumentParser(description='TnP Fitter')

    subparsers = parser.add_subparsers(help='Fitting step', dest='command')

    parser_convert = subparsers.add_parser(
        'convert',
        help='Convert ROOT to parquet',
    )
    add_common_particle(parser_convert)
    add_common_resonance(parser_convert)
    add_common_era(parser_convert)
    add_common_options(parser_convert)

    parser_flatten = subparsers.add_parser(
        'flatten',
        help='Flatten to histograms',
    )
    add_common_particle(parser_flatten)
    add_common_resonance(parser_flatten)
    add_common_era(parser_flatten)
    add_common_options(parser_flatten)
    add_common_flatten(parser_flatten)

    parser_fit = subparsers.add_parser(
        'fit',
        help='Fit pass/fail histograms',
    )
    add_common_particle(parser_fit)
    add_common_resonance(parser_fit)
    add_common_era(parser_fit)
    add_common_options(parser_fit)
    add_common_multi(parser_fit)
    add_common_fit(parser_fit)

    parser_plot = subparsers.add_parser(
        'plot',
        help='Plot fitted histograms',
    )
    add_common_particle(parser_plot)
    add_common_resonance(parser_plot)
    add_common_era(parser_plot)
    add_common_options(parser_fit)
    add_common_multi(parser_plot)
    add_common_fit(parser_plot)

    return parser.parse_args(argv)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_command_line(argv)

    job_fn = None
    unit = 'unit'
    desc = 'Processing'
    add_fn = None
    output = None

    if args.baseDir:
        baseDir = args.baseDir
    elif args.particle == 'muon':
        baseDir = os.path.join(
            '/eos/cms/store/group/phys_muon',
            f'{getpass.getuser()}/TagAndProbe',
        )
    else:
        baseDir = os.path.join(
            '/eos/cms/store/user',
            f'{getpass.getuser()}/TagAndProbe/{args.particle}',
        )

    if args.command == 'convert':
        raise NotImplementedError
    elif args.command == 'flatten':
        from flattener import run_spark
        run_spark(args.particle, args.resonance, args.era,
                  numerator=args.numerator, denominator=args.denominator,
                  shiftType=args.shiftType, baseDir=baseDir)
        return 0
    elif args.command == 'fit':
        from fitter import run_single_fit, build_fit_jobs
        job_fn = run_single_fit
        jobs = build_fit_jobs(
            baseDir,
            numerator=args.numerator,
            denominator=args.denominator,
            fitType=args.fitType,
            sampleType=args.sampleType,
            shiftType=args.shiftType,
            efficiencyBin=args.efficiencyBin,
        )
        unit = 'fit'
        desc = 'Fitting'
    elif args.command == 'plot':
        from plotter import plot, build_plot_jobs
        job_fn = plot
        jobs = build_plot_jobs(
            baseDir,
            numerator=args.numerator,
            denominator=args.denominator,
            fitType=args.fitType,
            sampleType=args.sampleType,
            shiftType=args.shiftType,
            efficiencyBin=args.efficiencyBin,
        )
        unit = 'plot'
        desc = 'Plotting'

    # TODO use condor or spark or parsl or dask...
    if args.dryrun:
        print('Will run {} {} jobs'.format(len(jobs), args.command))
    elif args.condor:
        submit_dir = './'
        joblist = os.path.join(submit_dir, 'joblist.txt')
        with open(joblist, 'w') as f:
            for job in jobs:
                f.write(','.join([str(j) for j in job])+'\n')
    elif args.workers > 1:
        import concurrent.futures
        with concurrent.futures.ProcessPoolExecutor(args.workers) as executor:
            futures = set(executor.submit(job_fn, *job) for job in jobs)
            _futures_handler(futures, status=True, unit=unit, desc=desc,
                             add_fn=add_fn, output=output)
    else:
        for job in tqdm(jobs, ncols=80, unit=unit, desc=desc):
            result = job_fn(*job)
            if add_fn is not None:
                add_fn(output, result)


if __name__ == "__main__":
    status = main()
    sys.exit(status)
