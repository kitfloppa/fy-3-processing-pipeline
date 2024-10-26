from setuptools import setup, find_packages

VERSION = '0.0.2' 
DESCRIPTION = 'My first Python package'
LONG_DESCRIPTION = 'My first Python package with a slightly longer description'

# Setting up
setup(
        name='fy3_utils', 
        version=VERSION,
        author='Andrey Timofeev, Vlad Kalinin',
        author_email='andrew20ggg@gmail.com',
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[], # add any additional packages that 
        
        # needs to be installed along with your package. Eg: 'caer'
        keywords=[],
        classifiers=[]
)