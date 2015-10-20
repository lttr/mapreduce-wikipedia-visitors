Vagrant.configure(2) do |config|
  config.vm.box = "hashicorp/precise64"
  config.vm.hostname = "test-hadoop.lttr.cz"
  config.vm.network "private_network", ip: "2.2.2.2"
  config.vm.provider "virtualbox" do |vb|
    vb.memory = "2048"
  end
end

