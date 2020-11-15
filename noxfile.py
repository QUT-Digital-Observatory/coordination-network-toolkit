import nox


@nox.session
def test(session):
    session.install('pytest')
    session.install('.')
    session.run('pytest')
