c.JupyterHub.authenticator_class = 'dummy'          # simple shared-password auth
c.DummyAuthenticator.password = 'letmein'           # change to any test password
c.Authenticator.admin_users = {'kaushalamancherla'} # give yourself admin rights
