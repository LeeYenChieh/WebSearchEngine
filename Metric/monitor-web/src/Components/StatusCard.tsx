import React from 'react';
import { Server, AlertCircle, CheckCircle2 } from 'lucide-react';
import type { StatusType } from '../types';

interface StatusCardProps {
    name: string;
    url: string;
    status: StatusType;
}

const StatusCard: React.FC<StatusCardProps> = ({ name, url, status }) => {
    return (
        <div className="bg-gray-900 p-5 rounded-lg shadow-lg border border-gray-800 flex items-center justify-between transition-colors hover:border-gray-700">
        <div className="flex items-center gap-4">
            <div
            className={`p-2 rounded-lg ${
                status === 'healthy'
                ? 'bg-emerald-900/30 text-emerald-400'
                : status === 'unhealthy'
                ? 'bg-red-900/30 text-red-400'
                : 'bg-gray-800 text-gray-400'
            }`}
            >
            <Server className="w-6 h-6" />
            </div>
            <div>
            <h3 className="font-bold text-gray-100 text-lg">{name}</h3>
            <p className="text-xs text-gray-500 font-mono mt-0.5">{url}</p>
            </div>
        </div>
        <div className="flex items-center gap-2">
            {status === 'loading' && <span className="text-gray-500 text-sm animate-pulse">Checking...</span>}
            {status === 'healthy' && (
            <span className="flex items-center gap-1.5 text-emerald-400 font-medium text-sm bg-emerald-900/20 px-3 py-1 rounded-full border border-emerald-900/50">
                <CheckCircle2 size={14} /> Healthy
            </span>
            )}
            {status === 'unhealthy' && (
            <span className="flex items-center gap-1.5 text-red-400 font-medium text-sm bg-red-900/20 px-3 py-1 rounded-full border border-red-900/50">
                <AlertCircle size={14} /> Unhealthy
            </span>
            )}
        </div>
        </div>
    );
};

export default StatusCard;