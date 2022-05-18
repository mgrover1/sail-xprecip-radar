# SAIL X-band Precipitation Radar (sail-xprecip-radar)

A repository for collaboration with ARM on the X-Band radar(s) deployed to Colorado for the Surface Atmosphere Integrated Field Laboratory (SAIL) campaign.

## Install the Environment

Before getting started with the materials in this repository, you will need to install the environment using the following steps:

1. Clone this repository

```
git clone https://github.com/ARM-Development/sail-xprecip-radar.git
```

1. Move into the new directory and install the enviroment (using conda or mamba)

```
conda env create -f environment.yml
```

## Use the Notebooks

You can interact with the notebooks by using the [JupyterBook](https://arm-development.github.io/sail-xprecip-radar/overview.html), which renders the notebooks and has an option to launch the notebooks on ARM cyberinfrastructure (by using the launch button at the top, and selecting launch JuptyerHub)

You can also launch these notebooks using your local environment by moving into the `notebooks` directory after starting your Jupyter session.
