from setuptools import setup

setup(
    name="ikon",
    version="0.0.1",
    description="A Python package for the People",
    author="Hugh Cameron",
    author_email="hescameron@gmail.com",
    url="https://github.com/hughcameron/ikon",
    packages=[
        "ikon"
    ],
    package_data={
        "ikon": [
            "config.ini"
        ]
    },
    include_package_data=True,
    keywords=[
        "python"
    ],
    install_requires=[
        'pytest', 'pandas', 'numpy', 'pyicu'
    ]
)
