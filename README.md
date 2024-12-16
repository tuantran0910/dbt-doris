<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg" alt="dbt logo" width="500"/>
</p>

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-doris

This repo contains the base code to help you start to build out your dbt-doris adapter plugin to work with an Apache Doris database. For more information on how to build out the adapter please follow the [docs](https://docs.getdbt.com/docs/contributing/building-a-new-adapter)

**Note** this `README` is meant to be replaced with what information would be required to use your adpater once your at a point todo so.

### Adapter Scaffold default Versioning

This adapter plugin follows [semantic versioning](https://semver.org/). The first version of this plugin is v0.1.0, in order to be compatible with dbt Core v1.3.0.

## Getting Started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Setting up Locally

- Clone this repo

```bash
git clone https://github.com/tntuan0910/dbt-doris
cd dbt-doris
```

- Follow the instruction in the [Installing uv](https://docs.astral.sh/uv/getting-started/installation/) to install `uv` package.

- Build the adapter

```bash
uv sync
```

## Contribute

See `CONTRIBUTING.md` for a detailed overview of contributing a code change to this adapter.

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/), or open [an issue](https://github.com/tntuan0910/dbt-doris/issues/new)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/tntuan0910/dbt-doris/blob/HEAD/CONTRIBUTING.md)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
