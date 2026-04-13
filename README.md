Coming soon

## Setting up an SSH tunnel

To set up an SSH tunnel, you can use the following command:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_framelab -C "framelab"
```

In our case, we called the key `framelab`

Copy the public key to the server:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519_framelab.pub user@server_ip
```

Then, add the configuration to your SSH config file (`~/.ssh/config`):

```bash
Host framelab
    HostName server_ip
    User user
    IdentityFile ~/.ssh/id_ed25519_framelab
    LocalForward 8080 localhost:8080
```
