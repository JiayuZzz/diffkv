import os

length = [20,100,1000,10000,100000,1000000]
value_size = [1024, 4096]
ordered_ratio = [1,0.9,0.5,0.8,0]

for l in length:
    for vs in value_size:
        t = 1000000//l
        print("vtable: length {0} size {1} times {2}",l,vs,t,flush=True)
        os.system("./vtable_proto --scan_length={0} --value_size={1} --scan_times={2}".format(l,vs,t))
        for o in ordered_ratio:
            print("regionkv: length {0} size {1} ordered ratio {2} times {3}",l,vs,o,t,flush=True)
            os.system("./hashkv_proto --scan_length={0} --value_size={1} --ordered_keys_ratio={2} --scan_times={3}".format(l,vs,o,t))

