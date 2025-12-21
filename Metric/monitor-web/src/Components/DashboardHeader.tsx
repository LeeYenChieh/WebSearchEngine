import React from 'react';
import type { TimeFrame } from '../types';

interface DashboardHeaderProps {
    timeFrame: TimeFrame;
    setTimeFrame: (t: TimeFrame) => void;
}

const DashboardHeader: React.FC<DashboardHeaderProps> = ({ timeFrame, setTimeFrame }) => {
    return (
        <header className="mb-8 flex flex-col md:flex-row justify-between items-start md:items-end gap-4">
            <div>
            <h1 className="text-3xl font-bold text-white tracking-tight">Crawler Dashboard</h1>
            <p className="text-gray-400 mt-1">Real-time monitoring and historical analysis</p>
            </div>

            {/* TimeFrame Control */}
            <div className="bg-gray-900 p-1 rounded-lg border border-gray-800 flex">
            {(['daily', 'weekly', 'monthly'] as TimeFrame[]).map((t) => (
                <button
                key={t}
                onClick={() => setTimeFrame(t)}
                className={`px-4 py-1.5 text-sm font-medium rounded-md transition-all ${
                    timeFrame === t
                    ? 'bg-gray-800 text-blue-400 shadow-sm border border-gray-700'
                    : 'text-gray-500 hover:text-gray-300'
                }`}
                >
                {t.charAt(0).toUpperCase() + t.slice(1)}
                </button>
            ))}
            </div>
        </header>
    );
};

export default DashboardHeader;