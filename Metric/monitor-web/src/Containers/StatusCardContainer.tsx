import React, { useState, useEffect } from 'react';
import StatusCard from '../Components/StatusCard';
import type { StatusType } from '../types';

interface Props {
    name: string;
    url: string;
    urlName: string;
}

const StatusCardContainer: React.FC<Props> = ({ name, url, urlName }) => {
    const [status, setStatus] = useState<StatusType>('loading');

    useEffect(() => {
        const checkHealth = async () => {
        try {
            const res = await fetch(url);
            setStatus(res.ok ? 'healthy' : 'unhealthy');
        } catch {
            setStatus('unhealthy');
        }
        };

        checkHealth();
    }, [url]);

    return <StatusCard name={name} url={urlName} status={status} />;
};

export default StatusCardContainer;