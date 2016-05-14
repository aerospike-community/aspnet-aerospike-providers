function GetItemExclusive(rec, ticks)
  if not aerospike:exists(rec) then
    return nil
  end
  
  local wasLocked = rec["Locked"]
   
  if wasLocked == 0 then
    rec["Locked"] = 1
    rec["LockId"] = rec["LockId"] + 1
    rec["LockTime"] = ticks
	record.set_ttl(rec, rec["SessionTimeout"])
    aerospike:update(rec)
  end

  return list {wasLocked, rec["LockId"], rec["LockTime"], rec["SessionTimeout"], rec["SessionItems"]}
end

function UpdateItemExclusive(rec, lockId, ttl, delItems, modItems)
  if rec["LockId"] == lockId then
    local m = rec["SessionItems"]
	
	for k in list.iterator(delItems) do
	  map.remove(m, k);
	end
	
    for k,v in map.pairs(modItems) do
      m[k] = v
    end
    
	rec["SessionItems"] = m
	rec["SessionTimeout"] = ttl
	rec["Locked"] = 0
    aerospike:update(rec)
  end
end

function ReleaseItemExclusive(rec, lockId, ttl)
  if rec["LockId"] == lockId then
	rec["SessionTimeout"] = ttl
    rec["Locked"] = 0
    aerospike:update(rec)
  end
end

function ResetItemTimeout(rec, ttl)
  if aerospike:exists(rec) then
	rec["SessionTimeout"] = ttl
    aerospike:update(rec)
  end
end

function RemoveItem(rec, lockId)
  if rec["LockId"] == lockId then
    aerospike:remove(rec)
  end
end
