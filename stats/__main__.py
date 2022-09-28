def pair_stats(pair):
    from scipy.stats import wilcoxon
    from stats import A12
    from stats.A12 import a12_paired
    pair_dict = dict()
    c1 = pair[1]
    c2 = pair[2]
    pair_dict['pair'] = ' Vs '.join(pair[0])
    pair_dict['pair_tuple'] = pair[0]
    try:
        pair_dict['wilcoxon'] = wilcoxon(c1, c2)
    except Exception as er:
        print(pair[0])
        print('wilcoxon')
        print(er)

    cd_vda_estimate, cd_vda_magnitude = A12.cliffs_delta_vd_a(c1, c2)
    # vda_estimate, vda_magnitude = VD_A.VD_A(c1, c2)
    # pair_dict['vda_estimate'] = vda_estimate
    # pair_dict['vda_magnitude'] = vda_magnitude
    pair_dict['vda_estimate'] = cd_vda_estimate
    pair_dict['vda_magnitude'] = cd_vda_magnitude
    # pair_dict['a12'] = a12(c1, c2)
    pair_dict['a12_paired'] = a12_paired(c1, c2)
    return pair_dict


def calculate_vda_wilcoxon_a12(df, columns, max_workers=8):
    import pandas as pd
    from utils.pool_executors import process_parallel_run

    pairs = [((c1, c2), df[c1].tolist(), df[c2].tolist())
             for i, c1 in enumerate(columns)
             for j, c2 in enumerate(columns[i + 1:])
             if c1 is not None and c2 is not None and c1 != c2]
    dict_arr = process_parallel_run(pair_stats, pairs, max_workers=max_workers, ignore_results=False)
    return pd.DataFrame(dict_arr)
