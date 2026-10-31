# Land-variable RMSE boxplots

`box_plot/` contains the manuscript LAI, ET, and soil moisture (SM) spatial
RMSE comparison for 1985–2014. E3SM v1/v2 triangle markers appear in ET and
SM only.

- [data/](data/): individual model/member RMSE values and outlier lists.
- [figures/](figures/): annual and seasonal figures in PDF and PNG.
- [scripts/](scripts/): calculation and plotting code.

From the repository root, redraw the figures from the bundled CSV without the original model files:

```bash
python -m pip install -r box_plot/scripts/requirements.txt
python box_plot/scripts/plot_paper_boxplots.py --season all
```

Outputs are saved in `box_plot/figures/reproduced/`.
