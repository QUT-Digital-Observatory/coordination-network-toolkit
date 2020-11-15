import setuptools

install_requires = [
    "urllib3",
    "regex",
    "requests",
    "networkx"
]

extras_require = {
    "development": [
        "nox",
        "sphinx",
        "pytest"
    ]
}

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="coordination_network_toolkit",
    author="Timothy Graham (Digital Media Research Centre, QUT) and QUT Digital Observatory",
    author_email="digitalobservatory@qut.edu.au",
    description="Tools for computing networks of coordinated behaviour on social media",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/digital-observatory/coordination_networks",
    license='MIT',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Sociology"
    ],
    keywords='social_science social_media_analysis similarity_networks',
    python_requires='>=3.6',
    setup_requires=['setuptools_scm'],
    use_scm_version=True,
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        'console_scripts': [
            'compute_networks=coordination_network_toolkit.__main__:main'
        ]
    }
)
