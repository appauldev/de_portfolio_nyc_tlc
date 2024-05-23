from setuptools import find_packages, setup

setup(
    name="de_portfolio_nyc_tlc",
    packages=find_packages(exclude=["de_portfolio_nyc_tlc_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
