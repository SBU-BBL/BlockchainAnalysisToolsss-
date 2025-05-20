# This code uses path compression to speed up union find.
# It also caches wallets the first time it sees them. This makes it faster at the cost of memory. 
# This code was heavily inspired by several of the techniques mentioned on this page: https://en.wikipedia.org/wiki/Disjoint-set_data_structure
# The path halving technique is thanks to "Worst Case Analysis of Set Union Algorithms" by Robert E. Tarjan and Jan van Leeuwen
# DEPENDENCY: This code heavily relies on the assumption that the IDs are somewhat contiguous. This allows for array based indexing - which offers a big speedup in finding parents.
import numpy as np
from numba import njit, int32, uint8


@njit(inline="always", fastmath=True, nogil=True, cache=True)
def find(parent, x):                       
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x

# merges on small id. path compression makes it so that rank and size become a bit redundant and make slight slowdowns, from my testing.
@njit(inline="always", fastmath=True, nogil=True, cache=True)
def union(parent, x, y):                   
    xr = find(parent, x)
    yr = find(parent, y)
    if xr != yr:
        if xr < yr:
            parent[yr] = xr
        else:
            parent[xr] = yr
    

@njit(cache=True)
def mapExisting(parent, present):
    """
    This approach helps save unneccessary work. Wallet IDs in this edge list are NOT contiguous - all outputs are not spent.
    Therefore, it would waste memory to store and search every wallet id even if it didnt happen. 
    """
    m = np.sum(present)
    roots   = np.empty(m, dtype=int32)
    wallets = np.empty(m, dtype=int32)

    j = 0
    for i in range(present.size):
        if present[i]:
            roots[j]   = parent[i] + 1
            wallets[j] = i + 1
            j += 1
    return roots, wallets

 
def _group_by_sortedsplit(roots, wallets):
    """
    This avoids explicit looping by taking advantage of the fact that these numbers are integers.
    This allows them to be split based off of their order rather than looping through the dataset. 
    Pretty big speedup in testing.
    """
    order = np.lexsort((wallets, roots))
    roots   = roots[order]
    wallets = wallets[order]

    boundaries = np.nonzero(np.diff(roots))[0] + 1
    return [grp.tolist() for grp in np.split(wallets, boundaries)]



@njit(fastmath=True, nogil=True, cache=True)
def _process_dense(tx_ids, wallet_idx):
    """
    Gets information needed via array 
    """
    n_wallets = wallet_idx.max() + 1
    n_txs     = tx_ids.max() + 1

    parent  = np.arange(n_wallets, dtype=int32)
    present = np.zeros(n_wallets,  dtype=uint8)
    tx_first = np.full(n_txs, -1, dtype=int32)      

    for k in range(tx_ids.size):
        t = tx_ids[k]
        w = wallet_idx[k]
        present[w] = 1

        f = tx_first[t]
        if f != -1:
            union(parent, f, w)
        else:
            tx_first[t] = w

    for i in range(n_wallets):                      \
        parent[i] = find(parent, i)

    return parent, present


def cscUnionFind(data: np.ndarray):
    """
    This method resulted in a <50% speedup to trad. graph libraries on this task. Mostly due to the indexing logic.
    """
    if data.size == 0:
        return []

    tx_ids     = data[:, 0].astype(np.int32, copy=False)
    wallet_idx = data[:, 1].astype(np.int32, copy=False) - 1

    parent, present      = _process_dense(tx_ids, wallet_idx)
    roots, wallets       = mapExisting(parent, present)
    unique_roots, group_assignments = np.unique(roots, return_inverse = True)
    return np.column_stack((wallets, group_assignments)).astype(np.int32)
