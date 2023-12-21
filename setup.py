from setuptools import setup, find_packages
from encadeador import __version__

long_description = "encadeador_pem"

requirements = []
with open("requirements.txt", "r") as fh:
    requirements = fh.readlines()


setup(
    name="encadeador_pem",
    version=__version__,
    author="Rogerio Alves",
    author_email="rogerioalves.ee@gmail.com",
    description="encadeador_pem",
    long_description=long_description,
    install_requires=requirements,
    packages=find_packages(),
    py_modules=["main", "encadeador"],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    entry_points="""
        [console_scripts]
        encadeador-pem=main:main
    """,
)
