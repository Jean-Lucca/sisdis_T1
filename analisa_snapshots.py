import os
import re
from collections import defaultdict

SNAPSHOT_DIR = './snapshots'
N = 3
OUTPUT_FILE = './snapshot_analysis.txt'

def read_snapshots():
    snapshots = defaultdict(dict)
    pattern = re.compile(r'process_(\d+)\.txt')

    for filename in os.listdir(SNAPSHOT_DIR):
        match = pattern.match(filename)
        if match:
            processId = int(match.group(1))
            filepath = os.path.join(SNAPSHOT_DIR, filename)
            with open(filepath, 'r') as f:
                snapshotData = None
                snapshotId = None
                for line in f:
                    if 'Snapshot' in line:
                        if snapshotData:
                            snapshots[snapshotId][processId] = snapshotData
                        snapshotId = int(line.strip().split(' ')[1])
                        snapshotData = {'ProcessId': processId, 'SnapshotId': snapshotId, 'Messages': []}
                    elif 'Estado:' in line:
                        snapshotData['State'] = int(line.strip().split(': ')[1])
                    elif 'Relógio Lógico:' in line:
                        snapshotData['Lcl'] = int(line.strip().split(': ')[1])
                    elif 'Timestamp de Requisição:' in line:
                        snapshotData['ReqTs'] = int(line.strip().split(': ')[1])
                    elif 'Waiting:' in line:
                        waiting_str = line.strip().split(': ')[1]
                        waiting_list = parse_waiting_list(waiting_str)
                        snapshotData['Waiting'] = waiting_list
                    elif 'NbrResps:' in line:
                        snapshotData['NbrResps'] = int(line.strip().split(': ')[1])
                    elif 'Mensagens:' in line:
                        continue  
                    elif 'respOK' in line:
                        snapshotData['Messages'].append(line.strip()) 
                if snapshotData:
                    snapshots[snapshotId][processId] = snapshotData 
    return snapshots

def parse_waiting_list(waiting_str):
    waiting_str = waiting_str.strip('[]')
    waiting_values = re.findall(r'\btrue\b|\bfalse\b', waiting_str)
    waiting_list = [value.lower() == 'true' for value in waiting_values]
    return waiting_list

def check_invariant_1(snapshot):
    # Inv  1:   no máximo um processo na SC.
    in_mx_count = sum(1 for s in snapshot.values() if s['State'] == 2)
    return in_mx_count <= 1

def check_invariant_2(snapshot):
    # inv  2:  se todos processos estão em "não quero a SC", então todos waitings tem que ser falsos e não deve haver mensagens
    all_no_mx = all(s['State'] == 0 for s in snapshot.values())
    if all_no_mx:
        for s in snapshot.values():
            if any(s['Waiting']) or s['Messages']:
                return False
        return True
    return True

def check_invariant_3(snapshot):
    # inv 3:   se um processo q está marcado como waiting em p, então p está na SC ou quer a SC
    for p_data in snapshot.values():
        p_state = p_data['State']
        p_waiting = p_data['Waiting']
        for q_id, waiting in enumerate(p_waiting):
            if waiting:
                if p_state not in [1, 2]:
                    return False
    return True

def check_invariant_4(snapshot):
    # inv 4:   se um processo q quer a seção crítica (nao entrou ainda),
    #             então o somatório de mensagens recebidas, de mensagens em transito e de, flags waiting para p em outros processos
    #             deve ser igual a N-1  (onde N é o número total de processos)
    for q_data in snapshot.values():
        if q_data['State'] == 1:
            count_waiting = 0
            count_messages = len(q_data['Messages'])
            for p_data in snapshot.values():
                if p_data['ProcessId'] != q_data['ProcessId']:
                    if q_data['ProcessId'] < len(p_data['Waiting']) and p_data['Waiting'][q_data['ProcessId']]:
                        count_waiting += 1
            nbr_resps = q_data.get('NbrResps', 0)
            total = nbr_resps + count_messages + count_waiting
            if total != N - 1:
                return False
    return True

def check_invariant_5(snapshot):
    # Invariante 5: Se um processo está na seção crítica, ele não deve estar marcado como waiting em outro processo.
    for p_data in snapshot.values():
        if p_data['State'] == 2:
            for q_data in snapshot.values():
                if p_data['ProcessId'] != q_data['ProcessId']:
                    if q_data['Waiting'][p_data['ProcessId']]:
                        return False
    return True

def check_invariant_6(snapshot):
    # Invariante 6: Se um processo está esperando pela SC, seu ReqTs deve ser maior que o ReqTs de todos os processos no estado noMX
    for p_data in snapshot.values():
        if p_data['State'] == 1:  # wantMX == 1
            for q_data in snapshot.values():
                if q_data['State'] == 0 and q_data['ReqTs'] >= p_data['ReqTs']:
                    return False
    return True

def analyze_snapshots(snapshots):
    with open(OUTPUT_FILE, 'w') as f:
        for snapshotId, snapshot in snapshots.items():
            result = f"\nAnalisando Snapshot {snapshotId}:\n"
            inv1 = check_invariant_1(snapshot)
            inv2 = check_invariant_2(snapshot)
            inv3 = check_invariant_3(snapshot)
            inv4 = check_invariant_4(snapshot)
            inv5 = check_invariant_5(snapshot)
            inv6 = check_invariant_6(snapshot)

            result += " - Invariante 1 mantida\n" if inv1 else " - Invariante 1 VIOLADA\n"
            result += " - Invariante 2 mantida\n" if inv2 else " - Invariante 2 VIOLADA\n"
            result += " - Invariante 3 mantida\n" if inv3 else " - Invariante 3 VIOLADA\n"
            result += " - Invariante 4 mantida\n" if inv4 else " - Invariante 4 VIOLADA\n"
            result += " - Invariante 5 mantida\n" if inv5 else " - Invariante 5 VIOLADA\n"
            result += " - Invariante 6 mantida\n" if inv6 else " - Invariante 6 VIOLADA\n"

            print(result)
            f.write(result)

if __name__ == "__main__":
    snapshots = read_snapshots()
    analyze_snapshots(snapshots)
