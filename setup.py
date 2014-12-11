from setuptools import setup

setup(
    name='backup',
    version='0.1',
    py_modules=['backup'],
    install_requires=[
        'Click>=3.2',
    ],
    entry_points='''
        [console_scripts]
        backup=backup:main
    ''',
)
