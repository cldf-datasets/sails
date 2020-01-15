from setuptools import setup


setup(
    name='cldfbench_sails',
    py_modules=['cldfbench_sails'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'sails=cldfbench_sails:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
