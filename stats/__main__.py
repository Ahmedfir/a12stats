import logging
from typing import List, Dict

import pandas as pd
from pandas import DataFrame
from scipy.stats import wilcoxon

from stats import A12
from stats.A12 import a12_paired


def pair_stats(pair, a12_rev=True):
    pair_dict = dict()
    c1 = pair[1]
    c2 = pair[2]
    pair_dict['pair'] = ' Vs '.join(pair[0])
    pair_dict['pair_tuple'] = pair[0]
    pair_dict['a12_rev'] = a12_rev
    pair_dict['x1'] = pair[0][0]
    pair_dict['x2'] = pair[0][1]
    try:
        # alternative: {"two-sided", "greater", "less"}
        pair_dict['wilcoxon'] = {'less': wilcoxon(c1, c2, alternative='less'),
                                 'greater': wilcoxon(c1, c2, alternative='greater'),
                                 'two-sided': wilcoxon(c1, c2, alternative='two-sided')}
    except Exception as er:
        print(pair[0])
        print('wilcoxon')
        print(er)

    cd_vda_estimate, cd_vda_magnitude = A12.cliffs_delta_vd_a(c1, c2, a12_rev)
    # vda_estimate, vda_magnitude = VD_A.VD_A(c1, c2)
    # pair_dict['vda_estimate'] = vda_estimate
    # pair_dict['vda_magnitude'] = vda_magnitude
    pair_dict['vda_estimate'] = cd_vda_estimate
    pair_dict['vda_magnitude'] = cd_vda_magnitude
    # pair_dict['a12'] = a12(c1, c2)
    pair_dict['a12_paired'] = a12_paired(c1, c2, a12_rev)
    return pair_dict


def group_by_to_list(df, y, group_by) -> List:
    d = dict(sorted(df.groupby(group_by)[y].apply(list).to_dict().items()))
    assert all(len(arr) == 1 for arr in d.values())
    return [v for arr in d.values() for v in arr]


def bp_stats(df: DataFrame, x: str, y: str, group_by_col: str, max_workers=8, a12_rev=True):
    from commons.pool_executors import process_parallel_run
    xs = df[x].unique().tolist()
    pairs = [((c1, c2), group_by_to_list(df[df[x] == c1], y, group_by_col),
              group_by_to_list(df[df[x] == c2], y, group_by_col))
             for i, c1 in enumerate(xs)
             for j, c2 in enumerate(xs[i + 1:])
             if c1 is not None and c2 is not None and c1 != c2]
    dict_arr = process_parallel_run(pair_stats, pairs, a12_rev, max_workers=max_workers, ignore_results=False)
    return pd.DataFrame(dict_arr)


def calculate_vda_wilcoxon_a12(df, columns, max_workers=8):
    from commons.pool_executors import process_parallel_run

    pairs = [((c1, c2), df[c1].tolist(), df[c2].tolist())
             for i, c1 in enumerate(columns)
             for j, c2 in enumerate(columns[i + 1:])
             if c1 is not None and c2 is not None and c1 != c2]
    dict_arr = process_parallel_run(pair_stats, pairs, max_workers=max_workers, ignore_results=False)
    return pd.DataFrame(dict_arr)


def df_to_pdf(df, file_path):
    import pdfkit as pdf
    # see https://stackoverflow.com/a/45708997/3014036
    tmp_path = file_path.replace('.pdf', '_tmp.html')
    df.to_html(tmp_path)
    pdf.from_file(tmp_path, file_path)
    import os
    os.remove(tmp_path)


def replace_all(s: str, replacements: Dict[str, str]):
    if str is None or replacements is None or len(replacements) == 0:
        return s
    res = s
    for o in replacements:
        res = res.replace(o, replacements[o])
    return res


def df_to_latex(df, y_order: List, file_path, latex_col='a12_paired', float_format="{:0.4f}", corner_label='Approach',
                caption=None, label: str = None, to_latex: Dict = None):
    # '{:.2e}'
    rev_cols = ['vda_estimate', 'a12_paired']
    covered = []
    latex_dict = {corner_label: list(map(lambda string: replace_all(string, to_latex), y_order[:-1]))}
    for x, tool_x in reversed(list(enumerate(y_order, start=2))[1:]):
        col_x = []
        for y, tool_y in enumerate(y_order[:-1]):
            xy_set = {tool_x, tool_y}
            if tool_x == tool_y or xy_set in covered:
                col_x.append('--')
            else:
                x_res = df[(df['x1'] == tool_y) & (df['x2'] == tool_x)]
                if len(x_res) == 1:
                    val = x_res[latex_col].tolist()[0]
                    if latex_col == 'wilcoxon':
                        assert isinstance(val, dict)
                        col_x.append(val['greater'].pvalue)
                    else:
                        col_x.append(val)
                    covered.append(xy_set)
                else:
                    x_res_rev = df[(df['x2'] == tool_y) & (df['x1'] == tool_x)]
                    assert len(x_res_rev) == 1, '{0} , {1} not found.'.format(tool_x, tool_y)
                    val = x_res_rev[latex_col].tolist()[0]
                    if latex_col in rev_cols:
                        col_x.append(1.0 - val)
                    elif latex_col == 'wilcoxon':
                        assert isinstance(val, dict)
                        col_x.append(val['less'].pvalue)
                    else:
                        logging.warning('{0} : column not reversed !'.format(latex_col))
                        col_x.append(val)
                    covered.append(xy_set)

        latex_dict[replace_all(tool_x, to_latex)] = col_x

    pd.DataFrame(latex_dict).to_latex(file_path, index=False, bold_rows=True,
                                      float_format=float_format.format if float_format is not None else None,
                                      escape=False, caption=caption, label=label)
