. ./settings.sh

echo "start of 0keys.sh"

echo "y \n" | ssh-keygen -q -t ed25519 -f /home/$user/.ssh/id_ed25519 -N ""
cd $home

for name in $all_nodes; do
  echo "yes \n" | sshpass -p hadoop ssh-copy-id -o StrictHostKeyChecking=no $user@$name
done

echo "end of 0keys.sh"
